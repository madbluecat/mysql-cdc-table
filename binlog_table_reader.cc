#include "storage/binlogdb/binlog_table_reader.h"
#include "my_dbug.h"

Binlog_table_reader::Binlog_table_reader(std::string &dbnam,
                                         std::string &tblnam, 
                                         std::string &binlog_index_filenam)
{
  m_dbnam = dbnam;
  m_tblnam = tblnam;
  m_binlog_index_filenam = binlog_index_filenam;

  m_binlog_index_filestream.open(m_binlog_index_filenam, std::ifstream::in);
  if (m_binlog_index_filestream.is_open()) {
    std::string binlog_name;
    for (std::getline(m_binlog_index_filestream, binlog_name); m_binlog_index_filestream.good();
         std::getline(m_binlog_index_filestream, binlog_name))
      m_binlog_filenames.push_back(binlog_name);
    m_binlog_index_filestream.close();
  }
  m_mysqlbinlog_file_reader = new Binlog_file_reader(true);
  m_current_binlog_index = 0;
  
}

Binlog_table_reader::Binlog_table_reader()
{
  m_mysqlbinlog_file_reader = new Binlog_file_reader(true);
  m_current_binlog_index = 0;
  m_fdle = nullptr;
  update_after_read_event_ptr = nullptr;
  m_td = nullptr;
};

Binlog_table_reader::~Binlog_table_reader()
{

}

bool Binlog_table_reader::set_binlog_index_file(const char *filename)
{

  DBUG_PRINT("info", ("set_binlog_index_file assign %s(%lu)", filename, strlen(filename)));

  m_binlog_index_filenam = std::string(filename);

  m_binlog_index_filestream.open(m_binlog_index_filenam, std::ifstream::in);
  if (m_binlog_index_filestream.is_open()) {
    std::string binlog_name;
    for (std::getline(m_binlog_index_filestream, binlog_name); m_binlog_index_filestream.good();
         std::getline(m_binlog_index_filestream, binlog_name))
      m_binlog_filenames.push_back(binlog_name);
    m_binlog_index_filestream.close();
  }

  switch_to_next_binlog_file();
  return true;
}

void Binlog_table_reader::set_target_table(const char *db, const char *table)
{

  m_dbnam.assign(db);
  m_tblnam.assign(table);
} 

bool Binlog_table_reader::set_cdc_table(TABLE *tbl)
{ 
  if (tbl == nullptr) 
    return false;

  m_cdc_table = tbl;
  return true;
}

bool Binlog_table_reader::switch_to_next_binlog_file(){
  DBUG_PRINT("info", ("switch_to_next_binlog_file open %luth/%lu", m_current_binlog_index, m_binlog_filenames.size()));
  if (m_current_binlog_index >= m_binlog_filenames.size())
    return false;

  auto current_binlog_name = m_binlog_filenames[m_current_binlog_index].c_str();
  
  if(m_mysqlbinlog_file_reader->is_open()){
    m_mysqlbinlog_file_reader->close();
  }
  
  DBUG_PRINT("info", ("switch_to_next_binlog_file open %luth %s", m_current_binlog_index, current_binlog_name));
  if (m_mysqlbinlog_file_reader->open(current_binlog_name, 0, &m_fdle)) {
    my_printf_error(
      2024,
      "init read error ",
      MYF(0));
    return false;
  }
  m_current_binlog_index++;
  return true;
}

int Binlog_table_reader::get_packaged_buf(uchar *buf)
{
  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();

  if(update_after_read_event_ptr != nullptr)
  {
    uchar *value = update_after_read_event_ptr;
    Bit_reader null_bits(value);
    value += (bitmap_bits_set(rows_after_image) + 7) / 8;
    // real data ptr
    DBUG_PRINT("info", ("set update after data"));
    DBUG_DUMP("info", (uchar *)(value), m_ostream.length() - 2);

    uint table_def_field_index = 0;
    for (Field **field = m_cdc_table->field; *field; field++) {
      const char *field_name = (*field)->field_name;
      auto field_type = (*field)->real_type();

      char sqltype[64];
      String buffer;
      buffer.set((char *)sqltype, 64, &my_charset_bin);
      (*field)->sql_type(buffer);

      DBUG_PRINT("info", ("name %s type %s is blob field %d metadata %d",
                field_name, buffer.c_ptr(), (*field)->is_flag_set(BLOB_FLAG),
                m_td->field_metadata(table_def_field_index)));

      if (strcmp(field_name, "__op") == 0) {
        std::int32_t x = 3;
        (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
          (uchar *)&x, (*field)->pack_length());
        continue;
      }
      else if (strcmp(field_name, "__gtid") == 0)
      {
        (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
          (uchar *)&m_current_gtid, (*field)->pack_length());
        continue;
      }
      else if (strcmp(field_name, "__tm") == 0)
      { 
        (*field)->store_timestamp(update_event_when);
        continue;
      }

      if(null_bits.get())
      {
        (*field)->set_default();
        (*field)->set_null();
        continue;
      }
      
      uint32_t pack_size = m_td->calc_field_size((uint)table_def_field_index, pointer_cast<const uchar *>(value));
      DBUG_DUMP("info", (uchar *)(value), pack_size);
      (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
          value, m_td->field_metadata(table_def_field_index));

      value += pack_size;
      table_def_field_index++;
    }
    update_after_read_event_ptr = nullptr;
    return 0;
  }

  for (;;) {
    Log_event *ev = m_mysqlbinlog_file_reader->read_event_object();
    if(ev == nullptr)
      if(switch_to_next_binlog_file())
        continue;
      else
        return HA_ERR_END_OF_FILE;

    DBUG_PRINT("info", ("event type %s", ev->get_type_str()));

    switch (ev->get_type_code())
    {
    case binary_log::WRITE_ROWS_EVENT:{
      Rows_log_event *wre = (Rows_log_event *)ev;
      DBUG_PRINT("info", ("write row event. table id %llu", wre->get_table_id().id()));
      if (wre->get_table_id().id() == m_table_id) 
      {
        m_ostream.length(0);
        wre->write_data_body(&m_ostream);
        DBUG_PRINT("info", ("find target write row event. table have %lu rows, datalength %lu", wre->get_width(), m_ostream.length()));
        DBUG_DUMP("info", (uchar *)(m_ostream.c_ptr()), m_ostream.length());
        uchar * value = (uchar *)m_ostream.c_ptr();
        
        // skip m_width
        value += net_field_length_size(value);
        // skip bitmaps
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // 跳过null_bits
        Bit_reader null_bits(value);
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // real data ptr
        DBUG_DUMP("info", (uchar *)(value), m_ostream.length() - 2);

        uint table_def_field_index = 0;
        for (Field **field = m_cdc_table->field; *field; field++) {
          const char *field_name = (*field)->field_name;
          auto field_type = (*field)->real_type();
          

          char sqltype[64];
          String buffer;
          buffer.set((char *)sqltype, 64, &my_charset_bin);
          (*field)->sql_type(buffer);

          DBUG_PRINT("info", ("name %s type %s is blob field %d metadata %d",
                    field_name, buffer.c_ptr(), (*field)->is_flag_set(BLOB_FLAG),
                    m_td->field_metadata(table_def_field_index)));

          if (strcmp(field_name, "__op") == 0) {
            std::int32_t x = 1;
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&x, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__gtid") == 0)
          {
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&m_current_gtid, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__tm") == 0)
          { 
            (*field)->store_timestamp((const timeval*)&(wre->common_header->when));
            continue;
          }

          if(null_bits.get())
          {
            (*field)->set_default();
            (*field)->set_null();
            continue;
          }
          uint32_t pack_size = m_td->calc_field_size((uint)table_def_field_index, pointer_cast<const uchar *>(value));
          DBUG_DUMP("info", (uchar *)(value), pack_size);
          (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
            value, m_td->field_metadata(table_def_field_index));
          // if ((*field)->store((const char *)value, (*field)->pack_length(), m_ostream.charset())) {
          //   if (!is_enum) return HA_ERR_CRASHED;
          // }
          value += pack_size;
          table_def_field_index++;
        }
        return 0;  
      }
      break;
    }

    case binary_log::DELETE_ROWS_EVENT:{
      Rows_log_event *wre = (Rows_log_event *)ev;
      DBUG_PRINT("info", ("delete rows event. table id %llu", wre->get_table_id().id()));
      if (wre->get_table_id().id() == m_table_id) 
      {
        m_ostream.length(0);
        wre->write_data_body(&m_ostream);
        DBUG_PRINT("info", ("find target delete row event. table have %lu rows, datalength %lu", wre->get_width(), m_ostream.length()));
        DBUG_DUMP("info", (uchar *)(m_ostream.c_ptr()), m_ostream.length());
        uchar * value = (uchar *)m_ostream.c_ptr();
        
        // skip m_width
        value += net_field_length_size(value);
        // skip bitmaps
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // 跳过null_bits
        Bit_reader null_bits(value);
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // real data ptr
        DBUG_DUMP("info", (uchar *)(value), m_ostream.length() - 2);

        uint table_def_field_index = 0;
        for (Field **field = m_cdc_table->field; *field; field++) {
          const char *field_name = (*field)->field_name;
          auto field_type = (*field)->real_type();
          

          DBUG_PRINT("info", ("name %s type %d ", field_name, field_type));

          if (strcmp(field_name, "__op") == 0) {
            std::int32_t x = 0;
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&x, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__gtid") == 0)
          {
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&m_current_gtid, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__tm") == 0)
          { 
            (*field)->store_timestamp((const timeval*)&(wre->common_header->when));
            continue;
          }

          if(null_bits.get())
          {
            (*field)->set_default();
            (*field)->set_null();
            continue;
          }
          
          uint32_t pack_size = m_td->calc_field_size((uint)table_def_field_index, pointer_cast<const uchar *>(value));
          DBUG_DUMP("info", (uchar *)(value), pack_size);
          (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
            value, m_td->field_metadata(table_def_field_index));
          // if ((*field)->store((const char *)value, (*field)->pack_length(), m_ostream.charset())) {
          //   if (!is_enum) return HA_ERR_CRASHED;
          // }
          value += pack_size;
          table_def_field_index++;
        }
        return 0;
      }
      break;
    }
    
    case binary_log::UPDATE_ROWS_EVENT:{
      Rows_log_event *wre = (Rows_log_event *)ev;
      DBUG_PRINT("info", ("update rows event. table id %llu", wre->get_table_id().id()));
      if (wre->get_table_id().id() == m_table_id) 
      {
        m_ostream.length(0);
        /* 
          update data body. |width buf|before bitmap|after bitmap|before nullbits|before data|after nullbits|after data
        */
        wre->write_data_body(&m_ostream);
        DBUG_PRINT("info", ("find target update row event. table have %lu rows, datalength %lu", wre->get_width(), m_ostream.length()));
        DBUG_DUMP("info", (uchar *)(m_ostream.c_ptr()), m_ostream.length());
        uchar * value = (uchar *)m_ostream.c_ptr();
        
        // skip m_width
        value += net_field_length_size(value);
        // skip before bitmaps
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // skip after bitmaps
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        
        Bit_reader null_bits(value);
        // skip before null_bits
        value += (bitmap_bits_set(wre->get_cols()) + 7) / 8;
        // real data ptr
        DBUG_DUMP("info", (uchar *)(value), m_ostream.length() - 2);

        uint table_def_field_index = 0;
        for (Field **field = m_cdc_table->field; *field; field++) 
        {
          const char *field_name = (*field)->field_name;
          auto field_type = (*field)->real_type();
          

          DBUG_PRINT("info", ("name %s type %d ", field_name, field_type));

          if (strcmp(field_name, "__op") == 0) {
            std::int32_t x = 2;
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&x, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__gtid") == 0)
          {
            (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]), (uchar *)&m_current_gtid, (*field)->pack_length());
            continue;
          }
          else if (strcmp(field_name, "__tm") == 0)
          { 
            (*field)->store_timestamp((const timeval*)&(wre->common_header->when));
            continue;
          }

          if(null_bits.get())
          {
            (*field)->set_default();
            (*field)->set_null();
            continue;
          }
          
          uint32_t pack_size = m_td->calc_field_size((uint)table_def_field_index, pointer_cast<const uchar *>(value));
          DBUG_DUMP("info", (uchar *)(value), pack_size);
          (*field)->unpack(buf + (*field)->offset(m_cdc_table->record[0]),
            value, m_td->field_metadata(table_def_field_index));
          // if ((*field)->store((const char *)value, (*field)->pack_length(), m_ostream.charset())) {
          //   if (!is_enum) return HA_ERR_CRASHED;
          // }
          value += pack_size;
          table_def_field_index++;
        }

        rows_after_image = wre->get_cols_ai();
        update_after_read_event_ptr = value;
        update_event_when = &(wre->common_header->when);
        return 0;
      }
      break;
    }

    case binary_log::TABLE_MAP_EVENT:{
      if (m_td != nullptr){
        delete m_td;
      }
      Table_map_log_event *tme = (Table_map_log_event *)ev;
      DBUG_PRINT("info", ("table map event. %s.%s id %llu",
                                        tme->get_db_name(),
                                        tme->get_table_name(),
                                        tme->get_table_id().id()));
      if(m_dbnam.compare(tme->get_db_name()) == 0 && 
         m_tblnam.compare(tme->get_table_name()) == 0)
        m_table_id = tme->get_table_id();
        m_td = new table_def(tme->m_coltype, tme->m_colcnt, tme->m_field_metadata,
                         tme->m_field_metadata_size, tme->m_null_bits, tme->m_flags);
      break;
    }

    case binary_log::GTID_LOG_EVENT:{
      Gtid_log_event *gtid_ev = (Gtid_log_event *)ev;

      m_current_gtid = gtid_ev->get_gno();
      DBUG_PRINT("info", ("gtid log event. gtid %lld", m_current_gtid));
      break;
    }

    // case binary_log::PREVIOUS_GTIDS_LOG_EVENT:{
    //   Previous_gtids_log_event *pge = (Previous_gtids_log_event *)ev;


    //   m_prev_gtids = pge->add_to_set(const Gtid_set *set);;
    //   DBUG_PRINT("info", ("previous gtids log event. gtids %lld", m_prev_gtids));
    //   break;
    // }

    default:
      break;
    }

    delete ev;
  }

  return HA_ERR_CRASHED;
}


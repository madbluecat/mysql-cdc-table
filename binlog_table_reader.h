#ifndef _binlog_table_reader_h
#define _binlog_table_reader_h

#include <fstream>
#include <stdlib.h>
#include <cstring>
#include <string>
#include "sql/binlog_reader.h"
#include "sql/basic_ostream.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_record.h"
#include "my_base.h"


class Binlog_table_reader
{
private:
  Table_id m_table_id;
  std::string m_dbnam;
  std::string m_tblnam;
  std::string m_binlog_index_filenam;
  std::ifstream m_binlog_index_filestream;
  std::vector<std::string> m_binlog_filenames;
  std::size_t m_current_binlog_index;
  Binlog_file_reader* m_mysqlbinlog_file_reader;
  Format_description_log_event *m_fdle;
  StringBuffer_ostream<20480> m_ostream;
  table_def *m_td;
  const MY_BITMAP *rows_after_image;
  const timeval* update_event_when;
  uchar *update_after_read_event_ptr;
  rpl_gno m_current_gtid;

  MEM_ROOT blobroot;
  
  TABLE *m_cdc_table;

public:
  Binlog_table_reader(std::string &dbnam,
                      std::string &tblnam, 
                      std::string &binlog_index_filenam);
  Binlog_table_reader();
  ~Binlog_table_reader();

  const Table_id &get_table_id() const { return m_table_id; }
  const char *get_table_name() const { return m_tblnam.c_str(); }
  const char *get_db_name() const { return m_dbnam.c_str(); }
  const char *get_binlog_index_file_name() const { return m_binlog_index_filenam.c_str(); }
  const char *get_current_binlog_file_name() const { return m_binlog_filenames[m_current_binlog_index].c_str(); }

  std::int32_t get_binlog_size() { return m_binlog_filenames.size(); }
  const char* get_binlog_filename(std::int32_t index) { return m_binlog_filenames[index].c_str(); }

  void set_binlog_index(std::int32_t index) { m_current_binlog_index = index; }
  bool set_binlog_index_file(const char *filename);
  void set_target_table(const char *db, const char *table);
  bool set_cdc_table(TABLE *tbl);

  int get_packaged_buf(uchar *buf);
  bool switch_to_next_binlog_file();
  };


#endif // _binlog_table_reader_h
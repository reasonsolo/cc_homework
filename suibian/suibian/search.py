from django.conf import settings
import happybase
import jieba

def get_table(tablename):
    conn = happybase.Connection(host = settings.HBASE_HOST,
                                port = settings.HBASE_PORT,
                                compat = '0.90')
    return conn.table(tablename)


def search(kwords_lst):
    ksegs = []
    for kwords in kwords_lst:
        segs = jieba.cut(kwords, cut_all = True)
        unicode_segs = []
        for seg in segs:
            unicode_segs.append(seg.encode('utf-8'))

        ksegs += unicode_segs
    ksegs = set(ksegs)

    index_table = get_table('IndexResult')
    hbase_records = {}
    search_results = []
    rows = index_table.rows(ksegs)
    row_key = settings.ROW_KEY

    for key, data in rows:
       raw_records = data[row_key].split('[')
       record = {}
       for raw_record in raw_records:
           url_count = raw_record.split()
           try:
               url = url_count[0].replace('"', '')
               record[url] = int(url_count[1].strip(']'))
               search_results.append(url)
           except Exception:
               pass
       hbase_records[key] = record

    # TODO: better search logic
    return set(search_results)







from django.conf import settings
import happybase
import jieba
import ranking

def search(kwords_lst):
    conn = happybase.Connection(host = settings.HBASE_HOST,
                                port = settings.HBASE_PORT,
                                compat = '0.90')
    ksegs = []
    for kwords in kwords_lst:
        segs = jieba.cut(kwords, cut_all = True)
        unicode_segs = []
        for seg in segs:
            unicode_segs.append(seg.encode('utf-8'))

        ksegs += unicode_segs
    ksegs = set(ksegs)

    # 500 is not the correct parameter
    # should pass the number of html documents in table 'WebData'
    result_urls = ranking.ranking(conn, ksegs, 500)
    url_table = conn.table('WebData')
    results = []
    for url in result_urls:
        row = url_table.row(url)
        title = row['content:title']
        results.append([url, title])

    return results
#    rows = url_table.rows(result_urls)
#    results = []
#    for url, data in rows:
#        title = data['content:title']
#        results.append([url, title])
#    return results

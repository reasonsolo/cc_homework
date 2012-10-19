from django.conf import settings
import math

# conn is the connection to hbase
# ksegs is the set of keywords
# N is the number of documents in table 'WebData'
# returns the ranked url set
def ranking(conn, ksegs, N):
    # 2d array
    term_frequency = {}
    document_frequency = {}
    document_score = {}

    index_table = conn.table('IndexResult')
    rows = index_table.rows(ksegs)
    row_key = settings.ROW_KEY
    search_results = []

    for key, data in rows:
        raw_records = data[row_key].split('[')
        document_frequency[key] = len(raw_records)
        record = {}
        for raw_record in raw_records:
            url_count = raw_record.split()
            try:
                url = url_count[0].replace('"','')
                record[url] = int(url_count[1].strip(']'))
                search_results.append(url)
            except Exception:
                pass
        term_frequency[key] = record

    # calculate score
    result_urls = set(search_results)
    for url in result_urls:
        score = 0
        for key in ksegs:
            df = 0
            try:
                df = document_frequency[key]
            except Exception:
                pass

            if df > 0:
                tf = 0
                try:
                    tf = term_frequency[key][url]
                except Exception:
                    pass

                if tf > 0:
                    score += (1 + math.log(tf, 10)) * math.log(N / df, 10)

        document_score[url] = score

    # sort the result urls by score, descend order
    result_urls = sorted(result_urls,
            cmp = lambda x, y : cmp(document_score[y], document_score[x]))

    #for debugging
    print("..................start printing document score................")
    for url in result_urls:
        print(document_score[url])
    print("..................end printing document score..................")

    return result_urls

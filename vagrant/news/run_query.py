import psycopg2

DBNAME = "news"


class Table:
    def __init__(self, row_data, headers):
        self.headers = headers
        self.row_data = row_data

    def export_to_txt(self, file_name, unit):
        with open(file_name + ".txt", 'a') as out:
            for row in self.row_data:
                for item in row:
                    out.write(str(item) + " ")
                out.write(unit + "\n\n")


def query_db(query_string):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query_string)
    row_data = c.fetchall()
    headers = []
    for h in c.description:
        headers.append(h[0])
    db.close()
    return row_data, headers


def top_three_articles():
    query = "SELECT articles.Title, Count(log.path) " \
            "FROM articles " \
            "JOIN log on CONCAT('/article/', articles.slug) = log.path " \
            "GROUP BY title " \
            "ORDER BY count DESC " \
            "LIMIT 3;"

    row_data, headers = query_db(query)
    top_articles = Table(row_data, headers)
    top_articles.export_to_txt('top_articles', 'views')


def top_authors():
    sub_query = "SELECT articles.author, count(path) " \
                "FROM articles " \
                "JOIN log ON CONCAT('/article/', articles.slug) = log.path " \
                "GROUP BY author"

    query = "SELECT authors.name, top_articles.count " \
            "FROM (" + sub_query + ") AS top_articles " \
            "JOIN authors ON authors.id = top_articles.author " \
            "ORDER BY top_articles.count DESC;"

    row_data, headers = query_db(query)
    top_articles = Table(row_data, headers)
    top_articles.export_to_txt('top_authors', 'views')


def errors():
    # CREATE VIEW count_logs AS
    # SELECT date(time), COUNT(id) AS
    # views FROM log
    # GROUP BY date;

    # CREATE VIEW count_errors AS
    # SELECT date(time), COUNT(id) AS
    # errors FROM log WHERE status = '404 NOT FOUND'
    # GROUP BY date(time);

    sub_query = "SELECT count_logs.date, (100.0*count_errors.errors/count_logs.views) AS percentage " \
                "FROM count_logs, count_errors " \
                "WHERE count_logs.date = count_errors.date"

    query = "SELECT * " \
            "FROM (" + sub_query + ") AS perc_err " \
            "WHERE perc_err.percentage > 1 " \
            "ORDER BY perc_err.percentage DESC;"

    row_data, headers = query_db(query)
    top_articles = Table(row_data, headers)
    top_articles.export_to_txt('errors', '%')

if __name__ == '__main__':
    top_authors()
    top_three_articles()
    errors()
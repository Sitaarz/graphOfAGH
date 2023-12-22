import psycopg2
import csv
class Visualizer:
    def __init__(self):
        pass

    def write_researchers_nodes_to_csv(self):
        pass


    def write_edges_connecting_researchers_to_csv(self):
        conn = psycopg2.connect("host = localhost user=postgres password=1234 port=5432")
        cur = conn.cursor()
        cur.execute("Select DISTINCT researchers.id, articles.id from "
                    "researchers inner join links "
                    "on researchers.id = links.researcher_id inner join articles on "
                    "links.article_id = articles.id WHERE researchers.id < articles.id;")

        row = cur.fetchone()

        with open("edges.csv", 'w', newline='') as file:
            csvWriter = csv.writer(file)

            while row is not None:
                csvWriter.writerow(row)
                row = cur.fetchone()


        cur.close()
        conn.close()

if __name__ == "__main__":
    visualiser = Visualizer()
    visualiser.write_edges_connecting_researchers_to_csv()
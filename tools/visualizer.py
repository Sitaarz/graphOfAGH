import psycopg2
import csv


class Visualizer:
    def __init__(self):
        pass

    def write_researchers_nodes_to_csv(self, filepath):
        conn = psycopg2.connect("host = localhost user=postgres password=1234 port=5432")
        cur = conn.cursor()
        cur.execute("SELECT r.id, r.name || ' ' || r.surname AS full_name, COUNT(l.article_id) AS art_num "
                    "FROM researchers AS r LEFT JOIN links AS l ON r.id = l.researcher_id "
                    "GROUP BY r.id ORDER BY r.id;")
        
        row = cur.fetchone()
        
        with open(filepath, 'w', newline='') as file:
            csvWriter = csv.writer(file)
            csvWriter.writerow(["Id", "Label", "Art_num"])
            
            while row is not None:
                csvWriter.writerow(row)
                row = cur.fetchone()
                
        cur.close()
        conn.close()
        

    def write_edges_connecting_researchers_to_csv(self, filepath):
        conn = psycopg2.connect("host = localhost user=postgres password=1234 port=5432")
        cur = conn.cursor()
        cur.execute("""SELECT
                           r1.id AS researcher1_id,
                           r2.id AS researcher2_id,
                           COUNT(DISTINCT l1.article_id) AS common_articles_count
                       FROM
                           researchers r1
                       JOIN
                           links l1 ON r1.id = l1.researcher_id
                       JOIN
                           links l2 ON l1.article_id = l2.article_id AND l1.researcher_id < l2.researcher_id
                       JOIN
                           researchers r2 ON l2.researcher_id = r2.id
                       GROUP BY
                           researcher1_id, researcher2_id
                       ORDER BY
                           researcher1_id, researcher2_id;
                    """)

        row = cur.fetchone()

        with open(filepath, 'w', newline='') as file:
            csvWriter = csv.writer(file)
            csvWriter.writerow(["Source", "Target", "Weight"])

            while row is not None:
                csvWriter.writerow(row)
                row = cur.fetchone()


        cur.close()
        conn.close()

if __name__ == "__main__":
    visualiser = Visualizer()
    visualiser.write_researchers_nodes_to_csv('./gephi/nodes.csv')
    visualiser.write_edges_connecting_researchers_to_csv('./gephi/edges.csv')
    
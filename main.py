from dataDownloader import Downloader

def main():
    URI = "https://badap.agh.edu.pl/autorzy"

    downloader = Downloader()
    downloader.get_users(URI)
    for id, link, name in downloader.users:
        print(id, link, name)
        
    print("\n\n\n")
    
    downloader.get_articles()
    # downloader.createTables()

if  __name__ == "__main__":
    main()


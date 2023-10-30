from dataDownloader import Downloader

def main():
    URI = "https://badap.agh.edu.pl/autorzy"

    downloader = Downloader(URI)
    downloader.getUsers()
    # downloader.createTables()

if  __name__ == "__main__":
    main()


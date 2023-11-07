from dataDownloader import Downloader

def main():
    URI = "https://badap.agh.edu.pl/autorzy"

    downloader = Downloader(URI)
    downloader.getUsers()
    # downloader.createTa
    # bles()

if  __name__ == "__main__":
    main()


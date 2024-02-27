# graphOfAGH
View all information in [technical_documentation.pdf](technical_documentation.pdf) (it is in polish).

<a href="https://lucid.app/lucidchart/ab6a9e49-a27c-4626-ac64-2e8ff4e4d3c3/edit?invitationId=inv_af7aa229-0787-4935-9dd9-a4802d975d57" target="_blank">ERD</a>

## How to run in terminal
Create `Downloader` object
```python
downloader = Downloader()
```

Create tables in database
```python
downloader.create_tables()
```

Download users from AGH
```python
downloader.get_users()
downloader.write_users_to_db()
```

Download articles from AGH
```python
downloader.get_and_write_articles()
```

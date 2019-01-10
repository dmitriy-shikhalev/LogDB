# LogDB
Column-based database on python3/async.<br/>
Github:
[Github-flavored Markdown](https://github.com/dmitriy-shikhalev/LogDB)<br/>
LogDB can only save row, not modify or remove.<br/>
There is no migrations - you must create a config file, there is no check for posible errors.<br/>
Usage: python -m LogDB config_file_name<br/>
Tests with work only when package is installed.<br/>
Client usage can be seen in LogDB/tests.py (append, append_many, read).<br/>
You can make filtering, but only on distinct=True field.
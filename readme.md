## Frege Analyzer C++
### Overview
This is an application responsible for analyzing <b>.cpp</b> files. 
It is responsible for accepting messages from queue <b>analyze-cpp</b> defined in [examples](#examples).
After receiving said message it looks for all the files that need to be analyzed - 
those connected with received repo_id that are written in <b>C++</b> 
(have value in <i>language_id</i> equal to <b>2</b>), are present and have not already been analyzed.
In order to analyze said files the application uses [lizard](https://pypi.org/project/lizard/). 
After completing the process application sends a message to <b>gc</b> queue in order to confirm that the process of 
analyzing have been completed. Example of said message can be seen in [examples](#examples).

The application connects to <b>frege</b> <i>postgresql</i> database and automatically creates table <b>cppfile</b> that
is used to store the results of analyzing process. The model of said table is defined in [database.py](frege-analyzer-cpp/database.py)

### Examples
#### Example of message received from analyze-cpp queue
```json
{ 
  "repo_id": "id" 
} 
```

#### Example of message send to gc queue
```json
{ 
  "repo_id": "id", 
  "language_id": 2 
} 
```

### Environment variables
In order to run this application locally it's required to supply it with below environment variables:

`RMQ_HOST` - The RabbitMQ host

`RMQ_PORT` - RabbitMQ port (if not specified default port 5672 is used)

`DB_HOST` - PostgreSQL server host

`DB_DATABASE` - Database name

`DB_USERNAME` - Database username

`DB_PASSWORD` - Database password

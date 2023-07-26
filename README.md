**SysPAD API - v1.1.0**

**Description**

The Syspad API (Application Programming Interface) consists of a RESTful (Representational State Transfer) model that enables the interaction of different systems efficiently through a set of architectural guidelines based on the HTTP protocol, implementing standardized methods such as GET, POST, PUT, and DELETE. The system has the function of protecting the database based on encryption and anonymization.

**Key Features**

- User
- Auth
- Password
- Valid database
- Database
- Anonymization type
- Anonymization record
- Encryption
- Anonymization
- Sql log

**Complete Documentation**

The complete API documentation, including details about all available endpoints, request parameters, response formats, and usage examples, can be found at [DOCUMENTATION LINK](https://github.com/FRIDA-LACNIC-UECE/documentation/blob/main/SysPAD%20Documentation.pdf) or SWAGGER DOCUMENTATION at http://localhost:5000 after executed.

**Technologies Used**

- Programming Language: Python (Version 3.10)
- Framework/API/Web Framework: Flask Framework
- Supported Database: MySQL and PostgreSQL
- Object Relational Mapping: SqlAlchemy

**Installation and Usage**

**Without docker:**  

1. Clone this repository to your local machine using the following command:

   ```
   git clone https://github.com/FRIDA-LACNIC-UECE/back-end.git
   ```

2. Navigate to the project directory:

   ```
   cd back-end
   ```

3. Install the necessary dependencies:

   ```
   pip3 install -r requirements.txt
   ```

4. Set up the environment variables:

   ```
   export FLASK_APP=application.py
   export ENV_NAME=[ENV NAME]
   export DATABASE_URL=[DATABASE URL]
   export MAIL_USERNAME=[MAIL USERNAME]
   export MAIL_PASSWORD=[MAIL PASSWORD]
   export SECRET_KEY=[SECRET KEY]
   ```

5. Start the server:

   ```
   python3 application.py
   ```

**With docker:** 
1. Set up the environment variables:

   ```
   export MAIL_USERNAME=mail_username
   export MAIL_PASSWORD=mail_password
   export SECRET_KEY=secret_key
   ```

2. Start the docker compose:

   ```
   docker compose up
   ```


**Contributing**

If you would like to contribute to the project, please follow these steps:

1. Fork this repository.
2. Create a branch for your feature (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add your feature'`).
4. Push the branch (`git push origin feature/your-feature`).
5. Open a pull request.

import os
import ydb

import string
import random
import time
import hashlib 

from pass_validation import password_check
from email_validation import email_check

driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
driver.wait(fail_fast=True, timeout=5)
pool = ydb.SessionPool(driver)


def id_generator(size, chars=string.ascii_uppercase + string.ascii_lowercase + string.digits + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def query_generator(userid, login, email, last_seen, password, registration_date, token):
    
    registration_date = int(time.time())
    last_seen = registration_date
    
    return  """
    INSERT INTO users (id, login, email, last_seen, password, registration_date, token)
    VALUES("{}", "{}", "{}",{}, "{}", {}, "{}");
    """.format(userid, login, email, last_seen, password, registration_date, token)

def check_query(login, email):
   return  """
   SELECT COUNT(*) FROM users WHERE login == "{}" OR email == "{}";
   """.format(login, email)

def execute_query(session):
  return session.transaction().execute(
    query,
    commit_tx=True,
    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
  )

def execute_query2(session):
  return session.transaction(ydb.SerializableReadWrite()).execute(
    query,
    commit_tx=True,
    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
  )

def handler(event, context):
  try:
    registration_date = int(time.time())
    last_seen = registration_date
    userid = id_generator(20)
    login = event['queryStringParameters']["login"]
    password = event['queryStringParameters']["password"]
    hashed_password = hashlib.sha512(password.encode('utf-8') + userid.encode('utf-8')).hexdigest()
    email = event['queryStringParameters']["email"]
    token = id_generator(80)

    if password_check(password)==False:
        return {
        'statusCode': 200,
        'body': {
          "message": "Wrong password.",
          "status": False
                }
            }

    if email_check(email)==False:
        return {
        'statusCode': 200,
        'body': {
          "message": "Wrong email.",
          "status": False
                }
            }

    global query
    query = check_query(login, email)
    login_email_matches = pool.retry_operation_sync(execute_query2)

    if login_email_matches[0].rows[0]["column0"] > 0:
      return {
        'statusCode': 200,
        'body': {
          "message": "Email or username already taken.",
          "status": False
                }
            }

    query = query_generator(userid, login, email, last_seen, hashed_password, registration_date, token)
    result = pool.retry_operation_sync(execute_query)

    return {
      'statusCode': 200,
      'body': {
        "message": "Welcome!",
        "name": login, 
        "token": token,
        "status": True
              }
            }
  except Exception as exception:
    return {
      'statusCode': 200,
      'body': {
        "message": "Something went wrong.",
        "error" : type(exception).__name__,
        "event" : event,
        "status": False
              }
            }
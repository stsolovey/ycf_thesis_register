def password_check(passwd):      
    if len(passwd) < 6:
        return False
          
    if len(passwd) > 20:
        return False

    else:
        return True
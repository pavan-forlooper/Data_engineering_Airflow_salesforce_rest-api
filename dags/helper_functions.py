def extract_account_type(attributes):
    try:
        account_type = attributes['type']
        return account_type
    except (KeyError, TypeError):
        return None

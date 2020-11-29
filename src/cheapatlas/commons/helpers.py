def _left(s, amount):
    return s[:amount]

def _right(s, amount):
    return s[-amount:]

def _mid(s, offset, amount):
    return s[offset:offset+amount]
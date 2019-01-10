
def sum(a, b):
    if isinstance(a, str):
        for type_ in (int, float):
            try:
                a = type_(a)
            except:
                pass
            else:
                break
    if isinstance(b, str):
        for type_ in (int, float):
            try:
                b = type_(b)
            except:
                pass
            else:
                break
    return a + b

def count(a, b):
    return a + 1

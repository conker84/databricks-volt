def add_method(cls):
    def decorator(func):
        func_name = func.__name__
        if func_name.startswith('_'):
            func_name = func_name.lstrip('_')
        setattr(cls, func_name, func)
        return func
    return decorator
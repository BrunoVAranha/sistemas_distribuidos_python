class Mensagem:
    def __init__(self, key, value, timestamp=None):
        if timestamp is None:
            self.key = key
            self.value = value
        else:
            self.key = key
            self.value = value
            self.timestamp = timestamp

import os

class LogManager:
    def __init__(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.filename = filename

    def append_message(self, offset, message):
        """Append message safely with UTF-8 encoding"""
        try:
            if offset is None or message is None:
                print(f"[ERROR] Invalid log entry: offset={offset}, message={message}")
                return False

            with open(self.filename, "a", encoding="utf-8") as f:
                f.write(f"{offset}:{message}\n")

            return True
        except Exception as e:
            print(f"[ERROR] Failed to write log: {e}")
            return False

    def read_up_to(self, hwm):
        messages = []
        if not os.path.exists(self.filename):
            return messages
        with open(self.filename, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(":", 1)
                if len(parts) == 2:
                    offset, msg = parts
                    if int(offset) <= hwm:
                        messages.append(msg)
        return messages
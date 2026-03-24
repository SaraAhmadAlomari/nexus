from abc import ABC, abstractmethod
from typing import Any, List, Union


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list) or len(data) == 0:
            return False
        for item in data:
            if not isinstance(item, (int, float)):
                return False
        return True

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Numeric data validation failed")
            count: int = len(data)
            total: Union[int, float] = sum(data)
            avg: float = total / count
            return f"Processed {count} numeric values, sum={total}, avg={avg}"
        except ZeroDivisionError:
            return "Error: Cannot process an empty list (Division by zero)"
        except ValueError as ve:
            return f"Validation Error: {ve}"
        except Exception as e:
            return f"Error :{e}"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str) or len(data) == 0:
            return False
        return True

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Text data validation failed")
            words = data.split()
            word_count = len(words)
            char_count = len(data)
            return (
                f"Processed text: {char_count} characters, {word_count} words"
                )
        except ValueError as e:
            return f"Validation Error: {e}"
        except Exception as e:
            return f"Error: {e}"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and len(data) > 0

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Log entry validation failed")
            if "ERROR" in data:
                part = data.split(':', 1)
                msg = part[1].strip() if len(part) > 1 else data
                return f"[ALERT] ERROR level detected: {msg}"
            elif "INFO" in data:
                part = data.split(':', 1)
                msg = part[1].strip() if len(part) > 1 else data
                return f"[INFO] INFO level detected: {msg}"
            return f"Log: {data}"
        except ValueError as ve:
            return f"Validation Error: {ve}"
        except Exception as e:
            return f"Error: {e}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    try:
        num_proc = NumericProcessor()
        num_data = [1, 2, 3, 4, 5]
        print("Initializing Numeric Processor...")
        print(f"Processing data: {num_data}")
        if num_proc.validate(num_data):
            print("Validation: Numeric data verified")
        print("Output: " + num_proc.format_output(num_proc.process(num_data)))

        text_proc = TextProcessor()
        text_data = "Hello Nexus World"
        print("\nInitializing Text Processor...")
        print(f"Processing data: \"{text_data}\"")
        if text_proc.validate(text_data):
            print("Validation: Text data verified")
        print(f"Output: "
              f"{text_proc.format_output(text_proc.process(text_data))}")

        log_proc = LogProcessor()
        log_data = "ERROR: Connection timeout"
        print("\nInitializing Log Processor...")
        print(f"Processing data: \"{log_data}\"")
        if log_proc.validate(log_data):
            print("Validation: Log entry verified")
        print("Output: " + log_proc.format_output(log_proc.process(log_data)))
    except Exception as e:
        print(f"Critical failure: {e}")

    try:
        print("\n=== Polymorphic Processing Demo ===")
        print("Processing multiple data types through same interface...")
        processors: List[DataProcessor] = [
            NumericProcessor(), TextProcessor(), LogProcessor()
            ]
        test_data = [[1, 2, 3], "Hello Nexus", "INFO: System ready"]
        for i in range(len(processors)):
            try:
                res = processors[i].process(test_data[i])
                print(f"Result {i + 1}: {processors[i].format_output(res)}")
            except Exception as e:
                print(f"Error: {e}")
        print("\nFoundation systems online. Nexus ready for advanced streams.")
    except Exception as e:
        print(f"Critical failure: {e}")

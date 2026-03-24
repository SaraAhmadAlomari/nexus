from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id = stream_id
        self.stream_type = stream_type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        if criteria == "high_priority":
            return ([
                item for item in data_batch
                if "error" in str(item) or
                (isinstance(item, (int, float)) and
                 item > 100)
                 ])
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id, "stream_type": self.stream_type}


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            readings_map: Dict[str, List[float]] = {}
            avg_lst: List[str] = []
            for d in data_batch:
                if isinstance(d, str) and ":" in d:
                    name, value = d.split(':')
                    val = float(value)
                    if name not in readings_map:
                        readings_map[name] = []
                    readings_map[name].append(val)
            for name, value in readings_map.items():
                avg = sum(value) / len(value)
                avg_lst.append(f"avg {name}: {avg:.1f}°C")
            if not avg_lst:
                return "Sensor analysis: No valid readings processed"
            return (
                f"Sensor analysis: {len(readings_map)} "
                f"readings processed, {avg_lst[0]}"
                )
        except Exception as e:
            return f"Sensor Error: {e}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["status"] = "Active"
        stats["sensor_type"] = "Hardware"
        return stats

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        if criteria == "high_priority":
            high_alerts = []
            for d in data_batch:
                try:
                    if isinstance(d, str) and ":" in d:
                        val = float(d.split(':')[1])
                        if val > 100:
                            high_alerts.append(d)
                except Exception:
                    continue
            return high_alerts
        return super().filter_data(data_batch, criteria)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        total = 0
        count = 0
        try:
            for d in data_batch:
                if isinstance(d, str) and ":" in d:
                    name, value = d.split(':')
                    val = float(value)
                    if name == 'sell':
                        total -= val
                    else:
                        total += val
                    count += 1
            sign = "+" if total >= 0 else ""
            if count == 0:
                return "Transaction analysis: No valid operations processed"
            return (
                f"Transaction analysis: {count} operations, "
                f"net flow: {sign}{total:.0f} units"
                )
        except Exception as e:
            return f"Transaction Error: {e}"

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        if criteria == "high_priority":
            high_val = []
            for d in data_batch:
                try:
                    if isinstance(d, str) and ":" in d:
                        val = float(d.split(':')[1])
                        if val > 100:
                            high_val.append(d)
                except Exception:
                    continue
            return high_val
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        count = 0
        error_count = 0
        try:
            for d in data_batch:
                if isinstance(d, str):
                    if d == 'error':
                        error_count += 1
                    count += 1
            if count == 0:
                return "Event analysis: No valid events processed"
            return (
                f"Event analysis: {count} events, {error_count} error detected"
                )
        except Exception as e:
            return f"Event Error: {e}"


class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream):
        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]):
        print("\n=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...")
        print("\nBatch 1 Results:")
        for i in range(len(self.streams)):
            result = self.streams[i].process_batch(batches[i])
            print(f"- {result}")


if __name__ == "__main__":
    try:
        s_ls = ['temp:22.5', 'humidity:65', 'pressure:1013']
        sensor = SensorStream('SENSOR_001')

        t_ls = ['buy:100', 'sell:150', 'buy:75']
        transaction = TransactionStream("TRANS_001")

        e_ls = ['login', 'error', 'logout']
        event = EventStream("EVENT_001")

        print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

        print("Initializing Sensor Stream...")
        print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
        print(f"Processing sensor batch: {s_ls}")
        print(sensor.process_batch(s_ls))

        print("\nInitializing Transaction Stream...")
        print(f"Stream ID: {transaction.stream_id}, "
              f"Type: {transaction.stream_type}")
        print(f"Processing transaction batch: {t_ls}")
        print(transaction.process_batch(t_ls))

        print("\nInitializing Event Stream...")
        print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
        print(f"Processing event batch: {e_ls}")
        print(event.process_batch(e_ls))

        manager = StreamProcessor()
        manager.add_stream(sensor)
        manager.add_stream(transaction)
        manager.add_stream(event)

        manager.process_all([s_ls, t_ls, e_ls])

        f_sensor = sensor.filter_data(s_ls, "high_priority")
        f_trans = transaction.filter_data(t_ls, "high_priority")

        print("\nStream filtering active: High-priority data only")
        print(f"Filtered results: {len(f_sensor)} critical sensor alerts, "
              f"{len(f_trans)} large transaction")
        print("All streams processed successfully. Nexus throughput optimal.")

    except Exception as e:
        print(f"An Error occurs: {e}")

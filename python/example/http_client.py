import requests
import pyarrow as pa
from pyarrow import ipc
from io import BytesIO


def main():
    # Create a RecordBatch
    data = [
        pa.array([1, 2, 3, 4]),
        pa.array([5, 6, 7, 8]),
    ]
    schema = pa.schema(
        [
            ("a", pa.int32()),
            ("b", pa.int32()),
        ]
    )
    batch = pa.RecordBatch.from_arrays(data, schema=schema)

    # Serialize the RecordBatch
    buf = BytesIO()
    writer = pa.ipc.new_stream(buf, batch.schema)
    writer.write_batch(batch)
    writer.close()
    serialized_batch = buf.getvalue()

    # Send the serialized RecordBatch to the server
    response = requests.post("http://localhost:8818/gcd", data=serialized_batch)

    # Deserialize the response
    reader = pa.ipc.open_stream(BytesIO(response.content))
    result_batches = [b for b in reader]

    # Print the result
    for batch in result_batches:
        print("res \n", batch)


if __name__ == "__main__":
    main()

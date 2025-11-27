import pyarrow as pa

from databend_udf.udf import (
    _type_str_to_arrow_field,
    _field_type_to_string,
    _input_process_func,
    _output_process_func,
    _arrow_field_to_string,
)


def test_vector_sql_generation():
    # Test nullable VECTOR (default)
    field = _type_str_to_arrow_field("VECTOR(1024)")
    sql_type = _arrow_field_to_string(field)
    assert sql_type == "VECTOR(1024)"

    # Test NOT NULL VECTOR
    field_not_null = _type_str_to_arrow_field("VECTOR(1024) NOT NULL")
    sql_type_not_null = _arrow_field_to_string(field_not_null)
    assert sql_type_not_null == "VECTOR(1024) NOT NULL"


def test_vector_type_parsing():
    field = _type_str_to_arrow_field("VECTOR(1024)")
    # Should be List type with metadata, not FixedSizeList
    assert pa.types.is_list(field.type)
    assert field.metadata[b"Extension"] == b"Vector"
    assert field.metadata[b"vector_size"] == b"1024"
    assert pa.types.is_float32(field.type.value_type)
    # Default is nullable
    assert field.nullable is True

    # Test NOT NULL
    field_not_null = _type_str_to_arrow_field("VECTOR(1024) NOT NULL")
    assert field_not_null.nullable is False


def test_vector_type_formatting():
    # Test that a List with VECTOR metadata is formatted as VECTOR(N)
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True)),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"1024",
        },
    )
    type_str = _field_type_to_string(field)
    assert type_str == "VECTOR(1024)"


def test_vector_input_processing():
    # Input processing should handle List (which is what VECTOR is physically)
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True)),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"3",
        },
    )
    func = _input_process_func(field)

    # Input is a list of floats
    data = [[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]]
    processed = func(data)
    assert processed == data


def test_vector_output_processing():
    # Output processing should handle List
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True)),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"3",
        },
    )
    func = _output_process_func(field)

    # Output is a list of floats
    data = [[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]]
    processed = func(data)
    assert processed == data

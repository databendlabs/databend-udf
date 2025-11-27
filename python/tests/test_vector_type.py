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
    assert pa.types.is_fixed_size_list(field.type)
    assert field.type.list_size == 1024
    assert pa.types.is_float32(field.type.value_type)
    assert field.nullable is True


def test_vector_type_formatting():
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=False), 1024),
        nullable=True,
    )
    type_str = _field_type_to_string(field)
    assert type_str == "VECTOR(1024)"


def test_vector_input_processing():
    field = pa.field(
        "", pa.list_(pa.field("item", pa.float32(), nullable=False), 3), nullable=True
    )
    func = _input_process_func(field)

    # Input is a list of floats
    input_data = [1.0, 2.0, 3.0]
    result = func(input_data)
    assert result == [1.0, 2.0, 3.0]

    # Input is None
    assert func(None) is None


def test_vector_output_processing():
    field = pa.field(
        "", pa.list_(pa.field("item", pa.float32(), nullable=False), 3), nullable=True
    )
    func = _output_process_func(field)

    # Output is a list of floats
    output_data = [1.0, 2.0, 3.0]
    result = func(output_data)
    assert result == [1.0, 2.0, 3.0]

    # Output is None
    assert func(None) is None

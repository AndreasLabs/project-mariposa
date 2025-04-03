#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array, BooleanArray, TimestampNanosecondArray};
    use std::sync::Arc;
    use arrow::compute;
    use crate::ptars::converters::convert_timestamps;
    use arrow_schema::Field;

    #[test]
    fn test_convert_timestamps() {
        let seconds_field = Arc::new(Field::new("seconds", arrow::datatypes::DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", arrow::datatypes::DataType::Int32, true));
        
        let seconds_array: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![
            1710330693i64, 1710330702i64, 0i64,
        ]));
        
        let nanos_array: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![
            1_000, 123_456_789, 0
        ]));
        
        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];
        let valid = vec![true, true, false];
        
        let results = convert_timestamps(&arrays, &valid);
        
        assert_eq!(results.len(), 3);
        
        // Expected values
        let expected: TimestampNanosecondArray = Int64Array::from(vec![
            1710330693i64 * 1_000_000_000i64 + 1_000i64,
            1710330702i64 * 1_000_000_000i64 + 123_456_789i64,
            0,
        ]).reinterpret_cast();
        
        let mask = BooleanArray::from(vec![false, false, true]);
        let expected_with_null = compute::nullif(&expected, &mask).unwrap();
        
        assert_eq!(
            results.as_ref().to_data(),
            expected_with_null.as_ref().to_data()
        );
    }

    #[test]
    fn test_convert_timestamps_empty() {
        let seconds_field = Arc::new(Field::new("seconds", arrow::datatypes::DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", arrow::datatypes::DataType::Int32, true));
        
        let seconds_array: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let nanos_array: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(Vec::<i32>::new()));
        
        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];
        let valid: Vec<bool> = vec![];
        
        let results = convert_timestamps(&arrays, &valid);
        
        assert_eq!(results.len(), 0);
        
        let expected: TimestampNanosecondArray = Int64Array::from(Vec::<i64>::new()).reinterpret_cast();
        
        assert_eq!(results.as_ref().to_data(), expected.to_data());
    }
} 
'''Unit tests for avro_schema_to_es_mapping module'''
import unittest
import json

from avro_schema_to_es_mapping import from_avro_schema_to_es_mapping


class TestAvroSchemaToESMapping(unittest.TestCase):
    '''Unit test class for avro_schema_to_es_mapping module'''

    def test_simple(self):
        '''Test a simple, one-level "record" schema'''
        expected = {"mappings": {
                "dynamic": "strict",
                "properties": {
                    "Name": {
                        "type": "keyword",
                        "ignore_above": 256,
                        "norms": False,
                        "index_options": "freqs"
                    },
                    "Age": {
                        "type": "integer"
                    }
                }
            }
        }
        actual = from_avro_schema_to_es_mapping("test/data/simple.avsc")
        self.assertEqual(actual, json.dumps(expected))

    def test_array(self):
        '''Conversion of a schema defining a simple array of integers'''
        expected = {"mappings": {
                "dynamic": "strict",
                "properties": {
                    "integers": {
                        "properties": {
                            "type": "integer"
                        }
                    }
                }
            }
        }
        actual = from_avro_schema_to_es_mapping("test/data/array.avsc")
        self.assertEqual(actual, json.dumps(expected))

    def test_union(self):
        '''Conversion of a schema defining a union of a nullable string'''
        expected = {"mappings": {
                "dynamic": "strict",
                "properties": {
                    "address": {
                        "properties": {
                            "type": "keyword",
                            "ignore_above": 256,
                            "norms": False,
                            "index_options": "freqs"
                        }
                    }
                }
            }
        }
        actual = from_avro_schema_to_es_mapping("test/data/union.avsc")
        self.assertEqual(actual, json.dumps(expected))


if __name__ == '__main__':
    unittest.main()

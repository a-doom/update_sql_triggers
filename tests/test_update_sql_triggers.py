import unittest
import update_sql_triggers as ut


class TestQueryGenerator(unittest.TestCase):

    def test_find_file_object_type_procedure(self):
        result = ut.find_object_type("sometext CREATE PROCEDURE some text")
        expected_result = ut.ObjectTypes.SQL_STORED_PROCEDURE
        self.assertEqual(result, expected_result)

    def test_find_file_object_type_procedure_lowcase(self):
        result = ut.find_object_type("sometext create procedure some text")
        expected_result = ut.ObjectTypes.SQL_STORED_PROCEDURE
        self.assertEqual(result, expected_result)

    def test_find_file_object_type_procedure_error(self):
        self.assertRaises(TypeError, ut.find_object_type("sometext create procedure some text"))

    def test_find_file_object_type_trigger(self):
        result = ut.find_object_type("CREATE triggEr some text")
        expected_result = ut.ObjectTypes.SQL_TRIGGER
        self.assertEqual(result, expected_result)

    def test_find_file_object_type_func(self):
        result = ut.find_object_type("CREATE function")
        expected_result = ut.ObjectTypes.SQL_SCALAR_FUNCTION
        self.assertEqual(result, expected_result)


class TestRstripEveryLine(unittest.TestCase):

    def test_rstrip_every_line_1(self):
        result = ut.rstrip_every_line("text    ")
        expected_result = "text"
        self.assertEqual(result, expected_result)

    def test_rstrip_every_line_2(self):
        result = ut.rstrip_every_line("text    \n text2 \n\ntext3")
        expected_result = "text\n text2\ntext3"
        self.assertEqual(result, expected_result)


class FindChangedObjects(unittest.TestCase):

    def create_sqlobjects_dict(self, range_, is_new=False, append_text=""):
        result = {}
        for i in range_:
            result["name{0}".format(i)] = ut.SqlObject(
                object_name="name{0}".format(i),
                type_desc=ut.ObjectTypes.SQL_TRIGGER,
                object_text="object_text{0}{1}".format(i, append_text))
            result["name{0}".format(i)].is_new = is_new
        return result

    def test_find_changed_objects_all_new(self):
        sql_objects = {}
        file_objects = self.create_sqlobjects_dict(range(0, 4))

        result = ut.find_changed_objects(sql_objects=sql_objects, file_objects=file_objects)
        expected_result = list(self.create_sqlobjects_dict(range(0, 4),  True).values())

        self.assertEquals(sorted(result, key=lambda x: x.object_name),
                          sorted(expected_result, key=lambda x: x.object_name))

    def test_find_changed_objects_half_new(self):
        sql_objects = self.create_sqlobjects_dict(range(0, 2))
        file_objects = self.create_sqlobjects_dict(range(0, 4))

        result = ut.find_changed_objects(sql_objects=sql_objects, file_objects=file_objects)
        expected_result = list(self.create_sqlobjects_dict(range(2, 4), True).values())

        self.assertEquals(sorted(result, key=lambda x: x.object_name),
                          sorted(expected_result, key=lambda x: x.object_name))


    def test_find_changed_objects_all_edit(self):
        sql_objects = self.create_sqlobjects_dict(range(0, 4), append_text="1")
        file_objects = self.create_sqlobjects_dict(range(0, 4))

        result = ut.find_changed_objects(sql_objects=sql_objects, file_objects=file_objects)
        expected_result = list(self.create_sqlobjects_dict(range(0, 4)).values())

        self.assertEquals(sorted(result, key=lambda x: x.object_name),
                          sorted(expected_result, key=lambda x: x.object_name))


    def test_find_changed_objects_all_edit_2(self):
        sql_objects = self.create_sqlobjects_dict(range(0, 4))
        file_objects = self.create_sqlobjects_dict(range(0, 4), append_text="1")

        result = ut.find_changed_objects(sql_objects=sql_objects, file_objects=file_objects)
        expected_result = list(self.create_sqlobjects_dict(range(0, 4), append_text="1").values())

        self.assertEquals(sorted(result, key=lambda x: x.object_name),
                          sorted(expected_result, key=lambda x: x.object_name))


    def test_find_changed_objects_half_edit_half_new(self):
        sql_objects = self.create_sqlobjects_dict(range(0, 2))
        file_objects = self.create_sqlobjects_dict(range(0, 4), append_text="1")

        result = ut.find_changed_objects(sql_objects=sql_objects, file_objects=file_objects)
        expected_result = list(self.create_sqlobjects_dict(range(0, 2), append_text="1").values())
        expected_result.extend(list(self.create_sqlobjects_dict(range(2, 4), append_text="1", is_new=True).values()))

        self.assertEquals(sorted(result, key=lambda x: x.object_name),
                          sorted(expected_result, key=lambda x: x.object_name))
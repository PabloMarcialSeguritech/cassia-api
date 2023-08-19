import unittest
from services import cat_service


class CatServiceTest(unittest.TestCase):

    def test_get_roles(self):
        print('> Entrando a test_get_roles <')
        valor_esperado = 'admin'
        response = cat_service.get_roles()
        json_obj = response.body
        print("obj_resp:", json_obj.decode())
        self.assertIn(valor_esperado, json_obj.decode(), "Host no encontrado no se encuentra en la respuesta")

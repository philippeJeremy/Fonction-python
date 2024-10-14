import unittest
from unittest.mock import MagicMock, patch
from fonction import DatabaseClient, FTPClient

class TestDatabaseClient(unittest.TestCase):
    def setUp(self):
        # Configuration de test pour PostgreSQL
        self.db_client = DatabaseClient(driver='postgresql', identifiant='user', password='pass', host='localhost', database='test_db')

    def test_connection(self):
        # Tester si la connexion est établie correctement
        self.assertIsNotNone(self.db_client.connecter())

    def tearDown(self):
        # Fermer la connexion après les tests
        self.db_client.deconnecter()

class TestFTPClient(unittest.TestCase):
    
    def setUp(self):
        # Configuration pour le client FTP
        self.ftp_client = FTPClient(ftp_server='ftp.example.com', ftp_user='user', ftp_password='password')

    @patch('ftplib.FTP')
    def test_connection(self, mock_ftp):
        # Mock de la méthode FTP pour tester la connexion sans toucher au vrai serveur FTP
        self.ftp_client.connecter()
        mock_ftp.assert_called_with('ftp.example.com')  # Vérifie si le serveur FTP a été appelé
        mock_ftp.return_value.login.assert_called_with(user='user', passwd='password')  # Vérifie le login

    @patch('ftplib.FTP')
    @patch('builtins.open', new_callable=MagicMock)
    def test_telecharger_fichier(self, mock_open, mock_ftp):
        # Simuler le téléchargement d'un fichier via FTP
        self.ftp_client.connecter()  # Établir une connexion
        self.ftp_client.telecharger_fichier('/repertoire', 'fichier.txt', chemin_repertoire_local='.', nom_du_fichier_local='fichier_local.txt')

        # Vérifie si le répertoire a été correctement changé
        mock_ftp.return_value.cwd.assert_called_with('/repertoire')
        
        # Vérifie si le fichier a bien été récupéré
        mock_ftp.return_value.retrbinary.assert_called_with('RETR fichier.txt', mock_open.return_value.write)

    @patch('ftplib.FTP')
    def test_trouver_dernier_fichier(self, mock_ftp):
        # Simuler le listing de fichiers FTP pour trouver le dernier fichier modifié
        self.ftp_client.connecter()

        # Créer une liste factice de fichiers avec des timestamps différents
        mock_ftp.return_value.nlst.return_value = ['fichier1.txt', 'fichier2.txt']
        mock_ftp.return_value.sendcmd.side_effect = [
            '213 20240101010101',  # Date pour fichier1.txt
            '213 20240201010101'   # Date plus récente pour fichier2.txt
        ]

        dernier_fichier = self.ftp_client.trouver_dernier_fichier()
        self.assertEqual(dernier_fichier, 'fichier2.txt')  # Le dernier fichier modifié doit être 'fichier2.txt'

    @patch('ftplib.FTP')
    def test_deconnexion(self, mock_ftp):
        # Tester la déconnexion du serveur FTP
        self.ftp_client.connecter()
        self.ftp_client.deconnecter()
        mock_ftp.return_value.quit.assert_called()  # Vérifie si la méthode quit() a été appelée



if __name__ == '__main__':
    unittest.main()

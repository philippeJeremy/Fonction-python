import os
import smtplib
import paramiko
import pandas as pd
from email import encoders
from ftplib import FTP, error_perm
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from sqlalchemy import create_engine
from paramiko import SFTPError, SSHException
from email.mime.multipart import MIMEMultipart

class DatabaseClient:
    def __init__(self, driver: str, identifiant: str, password: str, host: str, database: str):
        """
        Initialise la connexion à la base de données avec les informations fournies.
        
        :param driver: Type de base de données ('postgresql', 'mysql', 'sqlserver', etc.)
        :param identifiant: Identifiant de connexion
        :param password: Mot de passe de connexion
        :param host: Adresse du serveur de base de données
        :param database: Nom de la base de données à laquelle se connecter
        """
        self.driver = driver
        self.identifiant = identifiant
        self.password = password
        self.host = host
        self.database = database
        self.engine = None

    def connecter(self):
        """Établit la connexion à la base de données en fonction du type de driver."""
        try:
            conn_str = ''
            if self.driver == 'postgresql':
                conn_str = f'postgresql://{self.identifiant}:{self.password}@{self.host}:5432/{self.database}'
            elif self.driver == 'mysql':
                conn_str = f'mysql+pymysql://{self.identifiant}:{self.password}@{self.host}:3306/{self.database}'
            elif self.driver == 'sqlserver':
                conn_str = f'mssql+pyodbc://{self.identifiant}:{self.password}@{self.host}/{self.database}?driver=SQL+Server'

            # Crée l'engine SQLAlchemy
            self.engine = create_engine(conn_str)
            print(f"Connexion réussie à {self.driver} : {self.database} sur {self.host}")

        except Exception as e:
            print(f"Erreur lors de la connexion à la base de données : {e}")
            raise

    def deconnecter(self):
        """Ferme la connexion à la base de données."""
        if self.engine is not None:
            self.engine.dispose()
            print("Connexion fermée.")

    def executer_requete(self, query: str) -> pd.DataFrame:
        """
        Exécute une requête SQL et retourne le résultat sous forme de DataFrame.

        :param query: Requête SQL à exécuter
        :return: Résultat de la requête sous forme de DataFrame
        """
        try:
            if self.engine is None:
                raise Exception("La connexion à la base de données n'a pas été établie. Appelez 'connecter()' d'abord.")
            
            # Exécuter la requête SQL
            df = pd.read_sql(query, self.engine)
            print(f"Requête exécutée avec succès : {query}")
            return df

        except Exception as e:
            print(f"Erreur lors de l'exécution de la requête : {e}")
            raise


class FTPClient:
    def __init__(self, ftp_server, ftp_user, ftp_password):
        """
        Initialise la connexion FTP avec les informations de connexion.
        
        :param ftp_server: Adresse du serveur FTP
        :param ftp_user: Nom d'utilisateur FTP
        :param ftp_password: Mot de passe FTP
        """
        self.ftp_server = ftp_server
        self.ftp_user = ftp_user
        self.ftp_password = ftp_password
        self.ftp = None

    def connecter(self):
        """Se connecte au serveur FTP et s'authentifie."""
        try:
            self.ftp = FTP(self.ftp_server)
            self.ftp.login(user=self.ftp_user, passwd=self.ftp_password)
            print(f"Connexion réussie à {self.ftp_server}")
        except Exception as e:
            print(f"Erreur lors de la connexion au serveur FTP : {e}")
            raise

    def deconnecter(self):
        """Ferme la connexion FTP."""
        if self.ftp is not None:
            self.ftp.quit()
            print(f"Déconnexion du serveur FTP {self.ftp_server}")

    def telecharger_fichier(self, nom_du_repertoire_ftp, nom_du_fichier_ftp=None, chemin_repertoire_local=".", nom_du_fichier_local=None):
        """
        Télécharge un fichier depuis le serveur FTP. Si le nom du fichier FTP n'est pas spécifié,
        récupère le dernier fichier modifié dans le répertoire.

        :param nom_du_repertoire_ftp: Répertoire sur le serveur FTP
        :param nom_du_fichier_ftp: Nom du fichier à télécharger (si None, récupère le dernier fichier modifié)
        :param chemin_repertoire_local: Chemin du répertoire local pour enregistrer le fichier (par défaut dans le répertoire courant)
        :param nom_du_fichier_local: Nom du fichier local (par défaut même nom que le fichier FTP)
        """
        try:
            self.ftp.cwd(nom_du_repertoire_ftp)
            
            # Si le nom du fichier FTP n'est pas spécifié, récupérer le dernier fichier modifié
            if nom_du_fichier_ftp is None:
                nom_du_fichier_ftp = self.trouver_dernier_fichier()
                if nom_du_fichier_ftp is None:
                    print(f"Aucun fichier trouvé dans le répertoire {nom_du_repertoire_ftp}")
                    return  # Sortir de la fonction si aucun fichier n'est trouvé
            
            # Utilise le même nom pour le fichier local si non spécifié
            if nom_du_fichier_local is None:
                nom_du_fichier_local = nom_du_fichier_ftp

            # Crée le chemin complet pour le fichier local
            nouveau_nom_fichier_local = os.path.join(chemin_repertoire_local, nom_du_fichier_local)

            # Vérifier si le fichier existe sur le serveur avant d'ouvrir le fichier local
            if not self.verifier_existence_fichier(nom_du_fichier_ftp):
                print(f"Le fichier {nom_du_fichier_ftp} n'existe pas sur le serveur FTP.")
                return

            # Télécharge le fichier
            with open(nouveau_nom_fichier_local, "wb") as fichier_local:
                self.ftp.retrbinary(f"RETR {nom_du_fichier_ftp}", fichier_local.write)

            print(f"Fichier {nom_du_fichier_ftp} téléchargé et sauvegardé sous {nouveau_nom_fichier_local}")

        except error_perm as e_perm:
            print(f"Erreur de permission FTP: {e_perm}")
            raise
        except Exception as e:
            print(f"Erreur lors du téléchargement du fichier : {e}")
            raise

    def trouver_dernier_fichier(self):
        """Trouve le dernier fichier modifié dans le répertoire FTP courant."""
        try:
            fichiers = self.ftp.mlsd()  # Utilise MLSD pour lister les fichiers avec leurs métadonnées
            fichiers = [f for f in fichiers if f[1]['type'] == 'file']  # Filtrer seulement les fichiers
            if not fichiers:
                return None

            # Trouver le fichier avec la date de modification la plus récente
            dernier_fichier = max(fichiers, key=lambda f: f[1]['modify'])
            return dernier_fichier[0]  # Retourne seulement le nom du fichier

        except Exception as e:
            print(f"Erreur lors de la recherche du dernier fichier modifié: {e}")
            return None

    def verifier_existence_fichier(self, nom_du_fichier):
        """Vérifie si un fichier existe sur le serveur FTP."""
        try:
            fichiers = [f[0] for f in self.ftp.mlsd() if f[1]['type'] == 'file']
            return nom_du_fichier in fichiers
        except Exception as e:
            print(f"Erreur lors de la vérification de l'existence du fichier: {e}")
            return False

class SFTPClient:
    def __init__(self, sftp_server, sftp_user, sftp_password):
        self.sftp_server = sftp_server
        self.sftp_user = sftp_user
        self.sftp_password = sftp_password
        self.transport = None
        self.sftp = None

    def connecter(self):
        """Établit une connexion SFTP."""
        try:
            self.transport = paramiko.Transport((self.sftp_server, 22))  # Port par défaut pour SFTP
            self.transport.connect(username=self.sftp_user, password=self.sftp_password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            print("Connexion SFTP établie avec succès.")
        except SSHException as e:
            print(f"Erreur de connexion SSH: {e}")
        except Exception as e:
            print(f"Erreur lors de la connexion SFTP: {e}")

    def telecharger_fichier(self, chemin_serveur, nom_fichier_serveur, chemin_local='.', nom_fichier_local=None):
        """
        Télécharge un fichier depuis le serveur SFTP. Si le fichier distant n'existe pas,
        le fichier local ne sera pas créé ou modifié.

        :param chemin_serveur: Répertoire distant sur le serveur SFTP
        :param nom_fichier_serveur: Nom du fichier distant à télécharger
        :param chemin_local: Répertoire local pour enregistrer le fichier (par défaut, le répertoire courant)
        :param nom_fichier_local: Nom du fichier local (par défaut, même nom que le fichier distant)
        """
        if nom_fichier_local is None:
            nom_fichier_local = nom_fichier_serveur

        chemin_local_fichier = os.path.join(chemin_local, nom_fichier_local)

        try:
            # Vérifie si le fichier existe dans le répertoire distant
            if not self.verifier_existence_fichier(chemin_serveur, nom_fichier_serveur):
                print(f"Le fichier '{nom_fichier_serveur}' n'existe pas dans le répertoire '{chemin_serveur}' sur le serveur SFTP.")
                return  # Sortir de la fonction sans télécharger ni créer le fichier local
            
            # Télécharge le fichier
            self.sftp.get(os.path.join(chemin_serveur, nom_fichier_serveur), chemin_local_fichier)
            print(f"Fichier '{nom_fichier_serveur}' téléchargé avec succès sous '{chemin_local_fichier}'.")
        
        except SFTPError as e:
            print(f"Erreur SFTP lors du téléchargement du fichier: {e}")
        except Exception as e:
            import traceback
            print(f"Erreur: {e}")
            traceback.print_exc()

    def trouver_dernier_fichier(self, chemin_serveur):
        """Trouve le dernier fichier modifié dans un répertoire SFTP."""
        try:
            fichiers = self.sftp.listdir_attr(chemin_serveur)
            dernier_fichier = max(fichiers, key=lambda f: f.st_mtime)  # Trouver le fichier avec le plus grand timestamp
            return dernier_fichier.filename
        except SFTPError as e:
            print(f"Erreur lors de la récupération de la liste des fichiers: {e}")
        except Exception as e:
            print(f"Erreur: {e}")

    def verifier_existence_fichier(self, chemin_serveur, nom_fichier_serveur):
        """
        Vérifie si un fichier existe sur le serveur SFTP.

        :param chemin_serveur: Répertoire distant sur le serveur SFTP
        :param nom_fichier_serveur: Nom du fichier à vérifier
        :return: True si le fichier existe, sinon False
        """
        try:
            # Obtenir la liste des fichiers dans le répertoire
            fichiers = [f.filename for f in self.sftp.listdir_attr(chemin_serveur)]
            return nom_fichier_serveur in fichiers
        except SFTPError as e:
            print(f"Erreur lors de la vérification de l'existence du fichier: {e}")
            return False

    def deconnecter(self):
        """Déconnecte le client SFTP."""
        if self.sftp:
            self.sftp.close()
            print("Déconnexion SFTP réussie.")
        if self.transport:
            self.transport.close()
            print("Transport SFTP fermé.")

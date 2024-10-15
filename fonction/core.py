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
                    raise Exception(f"Aucun fichier trouvé dans le répertoire {nom_du_repertoire_ftp}")
                print(f"Fichier le plus récent trouvé : {nom_du_fichier_ftp}")
            
            # Utilise le même nom pour le fichier local si non spécifié
            if nom_du_fichier_local is None:
                nom_du_fichier_local = nom_du_fichier_ftp

            # Crée le chemin complet pour le fichier local
            nouveau_nom_fichier_local = os.path.join(chemin_repertoire_local, nom_du_fichier_local)

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
        """Télécharge un fichier depuis le serveur SFTP.
        
        :param chemin_serveur: Répertoire sur le serveur SFTP
        :param nom_fichier_serveur: Nom du fichier à télécharger (si None, récupère le dernier fichier modifié)
        :param chemin_local: Chemin du répertoire local pour enregistrer le fichier (par défaut dans le répertoire courant)
        :param nom_fichier_local: Nom du fichier local (par défaut même nom que le fichier FTP)
        """
        if nom_fichier_local is None:
            nom_fichier_local = nom_fichier_serveur  # Si aucun nom de fichier local n'est spécifié

        chemin_local_fichier = os.path.join(chemin_local, nom_fichier_local)

        try:
            self.sftp.get(os.path.join(chemin_serveur, nom_fichier_serveur), chemin_local_fichier)
            print(f"Fichier '{nom_fichier_serveur}' téléchargé avec succès sous '{chemin_local_fichier}'.")
        except SFTPError as e:
            print(f"Erreur lors du téléchargement du fichier: {e}")
        except Exception as e:
            print(f"Erreur: {e}")

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

    def deconnecter(self):
        """Déconnecte le client SFTP."""
        if self.sftp:
            self.sftp.close()
            print("Déconnexion SFTP réussie.")
        if self.transport:
            self.transport.close()
            print("Transport SFTP fermé.")
            
class EmailClient:
    def __init__(self, smtp_server, smtp_port, email_user, email_password, smtp_tls=True):
        """
        Initialise le client email.

        Args:
            smtp_server (str): Adresse du serveur SMTP (ex: smtp.gmail.com).
            smtp_port (int): Port SMTP (ex: 587 pour TLS).
            email_user (str): Adresse email de l'expéditeur.
            email_password (str): Mot de passe de l'email ou un token d'authentification.
            smtp_tls (bool): Utiliser TLS pour la connexion sécurisée. Par défaut, True.
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email_user = email_user
        self.email_password = email_password
        self.smtp_tls = smtp_tls
        self.server = None

    def connecter(self):
        """Établit une connexion sécurisée au serveur SMTP."""
        try:
            self.server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.server.ehlo()
            if self.smtp_tls:
                self.server.starttls()
                self.server.ehlo()
            self.server.login(self.email_user, self.email_password)
            print("Connexion au serveur SMTP établie avec succès.")
        except smtplib.SMTPException as e:
            print(f"Erreur lors de la connexion au serveur SMTP: {e}")

    def envoyer_email(self, destinataires, sujet, corps_message, fichier_joint=None):
        """
        Envoie un email à un ou plusieurs destinataires avec ou sans pièce jointe.

        Args:
            destinataires (str ou list): Adresse email ou liste d'adresses email des destinataires.
            sujet (str): Sujet de l'email.
            corps_message (str): Contenu de l'email.
            fichier_joint (str, facultatif): Chemin vers un fichier à joindre. Par défaut, None.
        """
        if isinstance(destinataires, str):
            destinataires = [destinataires]

        # Créer l'objet email
        email = MIMEMultipart()
        email['From'] = self.email_user
        email['To'] = ', '.join(destinataires)
        email['Subject'] = sujet

        # Attacher le corps du message
        email.attach(MIMEText(corps_message, 'plain'))

        # Ajout d'une pièce jointe si spécifiée
        if fichier_joint:
            try:
                with open(fichier_joint, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(fichier_joint)}')
                email.attach(part)
                print(f"Pièce jointe '{fichier_joint}' ajoutée avec succès.")
            except Exception as e:
                print(f"Erreur lors de l'ajout de la pièce jointe: {e}")

        # Envoi de l'email
        try:
            self.server.sendmail(self.email_user, destinataires, email.as_string())
            print(f"Email envoyé avec succès à {', '.join(destinataires)}.")
        except smtplib.SMTPException as e:
            print(f"Erreur lors de l'envoi de l'email: {e}")

    def deconnecter(self):
        """Ferme la connexion au serveur SMTP."""
        if self.server:
            self.server.quit()
            print("Déconnexion du serveur SMTP.")
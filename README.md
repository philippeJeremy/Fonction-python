Description
Ce projet contient deux classes Python, DatabaseClient et FTPClient, SFTPClient, EmailClient qui permettent respectivement de :

Gérer des connexions à des bases de données (PostgreSQL, MySQL, SQL Server) et exécuter des requêtes SQL.
Télécharger des fichiers via FTP, avec la possibilité de récupérer le dernier fichier modifié dans un répertoire FTP.
Télécharger des fichiers via SFTP, avec la possibilité de récupérer le dernier fichier modifié dans un répertoire SFTP.
Evoie d'email selon les différents services de messagerie (Gmail,Outlook,Yahoo,ProtonMail).

# Exemple d'utilisation DatabaseClient

from fonction import DatabaseClient

## Création d'une instance de DatabaseClient pour PostgreSQL
client_db = DatabaseClient(
    driver='postgresql',        # Type de base de données ('postgresql', 'mysql', 'sqlserver')
    identifiant='username',     # Identifiant de connexion
    password='password',        # Mot de passe
    host='localhost',           # Adresse du serveur
    database='my_database'      # Nom de la base de données
)

## Connexion à la base de données
client_db.connecter()

## Exécution d'une requête SQL
df = client_db.executer_requete("SELECT * FROM table_name")
print(df)

## Fermeture de la connexion
client_db.deconnecter()

# Exemple d'utilisation FTPClient

from fonction import FTPClient

## Création d'une instance de FTPClient
client_ftp = FTPClient(
    ftp_server='ftp.example.com',  # Adresse du serveur FTP
    ftp_user='username',           # Nom d'utilisateur FTP
    ftp_password='password'        # Mot de passe FTP
)
## Connexion au serveur FTP
client_ftp.connecter()

## Téléchargement d'un fichier spécifique depuis un répertoire FTP
client_ftp.telecharger_fichier(
    nom_du_repertoire_ftp='/repertoire',         # Répertoire sur le serveur FTP
    nom_du_fichier_ftp='mon_fichier.txt',        # Nom du fichier à télécharger
    chemin_repertoire_local='/chemin/local',     # Chemin local pour enregistrer le fichier
    nom_du_fichier_local='fichier_local.txt'     # Nom local du fichier (optionnel)
)

## Téléchargement du dernier fichier modifié dans un répertoire FTP
client_ftp.telecharger_fichier(
    nom_du_repertoire_ftp='/repertoire',        # Répertoire sur le serveur FTP
    chemin_repertoire_local='/chemin/local'     # Chemin local pour enregistrer le fichier
)

## Déconnexion du serveur FTP
client_ftp.deconnecter()

# Exemple d'utilisation SFTPClient

from fonction import SFTPClient 

## Création d'une instance de FTPClient
sftp_client = SFTPClient(sftp_server='sftp.example.com', sftp_user='user', sftp_password='password')

## Connexion au serveur SFTP
sftp_client.connecter()

## Téléchargement d'un fichier spécifique depuis un répertoire SFTP
sftp_client.telecharger_fichier('/remote/directory', 'file.txt', chemin_local='.', nom_fichier_local='downloaded_file.txt')

## Téléchargement du dernier fichier modifié dans un répertoire SFTP
dernier_fichier = sftp_client.trouver_dernier_fichier('/remote/directory')

## Déconnexion du serveur SFTP
sftp_client.deconnecter()

# Exemple d'utilisation EmailClient

## Configuration pour Gmail (par exemple)
email_client = EmailClient(smtp_server="smtp.gmail.com", smtp_port=587, email_user="tonemail@gmail.com", email_password="tonmotdepasse")

email_client.connecter()

## Envoi d'un email avec pièce jointe à plusieurs destinataires
email_client.envoyer_email(
    destinataires=["destinataire1@example.com", "destinataire2@example.com"],
    sujet="Exemple d'email avec pièce jointe",
    corps_message="Bonjour, voici un fichier en pièce jointe.",
    fichiers_joints=["chemin/vers/fichier.pdf"]
)

email_client.deconnecter()

## Les différents services de messagerie nécessitent différentes configurations SMTP :

Gmail : smtp.gmail.com, port 587 (ou 465 pour SSL)
Outlook : smtp.office365.com, port 587
Yahoo : smtp.mail.yahoo.com, port 465 ou 587
ProtonMail : 127.0.0.1 (si configuration via bridge ProtonMail)
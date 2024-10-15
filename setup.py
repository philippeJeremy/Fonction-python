from setuptools import setup, find_packages

setup(
    name="fonction_utils",  # Nom de ton module, renommé pour inclure FTP et SQL
    version="0.1.0",  # Version initiale
    packages=find_packages(),  # Trouve et inclut automatiquement tous les packages
    install_requires=[  # Liste des dépendances
        "pandas",
        "paramiko",
        "SQLAlchemy",
        "python-dotenv",
        "pymysql",  
        "pyodbc",  
    ],
    description="Un module pour exécuter des requêtes SQL et télécharger des fichiers via FTP.",
    author="Ton Nom",
    author_email="tonemail@example.com",
    url="https://github.com/tonpseudo/database_ftp_utils",  # URL vers ton projet (si applicable)
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',  # Version minimum de Python
)

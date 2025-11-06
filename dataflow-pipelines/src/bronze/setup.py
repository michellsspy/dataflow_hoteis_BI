# src/bronze/setup.py

import setuptools

# A pasta raiz do projeto (dataflow-pipelines) é o diretório de contexto do Docker/Cloud Build.
# O Dockerfile copia 'src' para o WORKDIR.
# O pacote é 'bronze'.

setuptools.setup(
    name='dataflow-hotelaria-bronze',
    version='1.0.0',
    packages=setuptools.find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        # Dependências de runtime. Geralmente, você pode deixar o requirements.txt cuidar disso,
        # mas incluir o beam aqui garante a instalação do SDK.
        'apache-beam[gcp]>=2.50.0',
    ],
    # Pacotes de dependência
    # Aqui listamos os pacotes que o Dataflow deve empacotar.
    # Garantimos que 'bronze' seja incluído, permitindo imports relativos.
    include_package_data=True,
    description='Pipeline Apache Beam para ingestão da Camada Bronze da rede hoteleira.',
    author='Seu Nome/Time',
    license='MIT'
)
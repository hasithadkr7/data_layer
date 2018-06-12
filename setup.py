from setuptools import setup, find_packages

setup(
    name='data_layer',
    version='0.0.1',
    packages=find_packages(),
    url='http://www.curwsl.org',
    license='MIT',
    author='thilinamad',
    author_email='madumalt@gamil.com',
    description='Data Layer Abstraction of Center for Urban Waters, Sri Lanka',
    install_requires=['pymysql',
                      'SQLAlchemy',
                      'pytz',
                      'pandas',
                      'numpy'],
    zip_safe=False
)

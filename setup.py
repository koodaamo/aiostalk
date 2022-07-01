from setuptools import setup

setup(
    name='aiostalk',
    version='1.1',
    description='A Python 3 asyncio client for the beanstalkd work queue',
    long_description=open('README.rst').read(),
    author='Petri Savolainen',
    author_email='petri@koodaamo.fi',
    url='https://github.com/koodaamo/aiostalk',
    license='MIT',
    py_modules=['aiostalk'],
    install_requires=['greenstalk'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)

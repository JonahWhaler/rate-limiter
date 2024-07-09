from setuptools import setup, find_packages

DESCRIPTION = 'Another Python package aim to offer "Rate Limiting" functionality for general use cases.'

# python3 setup.py sdist bdist_wheel
# twine upload --skip-existing dist/* --verbose

VERSION = '0.0.6.1'

setup(
    name='pygrl',
    version=VERSION,
    packages=find_packages(),
    description=DESCRIPTION,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='jonah_whaler_2348',
    author_email='jk_saga@proton.me',
    license='MIT',
    install_requires=[],
    keywords=[
        'python', 'rate', 'limiter', 'rate limiter', 'rate limiting'
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
    ]
)

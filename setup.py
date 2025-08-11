from setuptools import setup, find_packages


setup(
    name='qtask', 
    version='0.1.6', 
    packages=find_packages(),
    description='A mini Task Scheduler',
    install_requires = [
        'redis>=4.0.0',
        'fastapi>=0.68.0',
        'uvicorn[standard]>=0.15.0',
        'pydantic>=1.8.0',
        'click>=8.0.0',
        'python-multipart>=0.0.5'],
    entry_points={
        "console_scripts": [
            "qtask=qtask.cli.commands:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "qtask.web": ["static/*", "templates/*"],
    },
    python_requires = '>=3',
    author='Liu Shengli',
    url='http://github.com/gseismic/qtask',
    zip_safe=False,
    author_email='liushengli203@163.com'
)

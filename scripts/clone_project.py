# stdlib
from pathlib import Path, PosixPath
from typing import Dict

# third party
import yaml


# Config
CLUSTER_TENANT_DICT = {
    'tenants_env1': ['cust1', 'cust2', 'cust3'],
    'tenants_env2': ['cust4'],
    'tenants_env3': ['cust5', 'cust6'],
}
MASTER_PROJECT_NAME = 'master_project'
DIRECTORIES = ['models', 'macros']

         
class ParseDirectory:
    def __init__(
        self,
        tenant_directory: PosixPath,
        directory: str,
        customer: str,
    ):
        self.tenant_directory = tenant_directory
        self.directory = directory
        self.customer = customer
        self.core_directory_path = tenant_directory / f'dbt_packages/{MASTER_PROJECT_NAME}/{directory}'
        self.files = [f for f in self.core_directory_path.rglob('*') if not f.is_dir()]    
    
    def run(self):
        for file in self.files:
            parser = None
            if file.suffix == '.sql':
                if self.directory in ['models', 'seeds', 'snapshots']:
                    parser = ModelParser
                elif self.directory == 'macros':
                    parser = MacroParser
            elif file.suffix == '.yml' or file.suffix == '.yaml':
                parser = SchemaParser
            elif file.suffix == '.md':
                parser = DocParser
            else:
                print(f'Skipping {file.name}.  {file} not yet supported')
            if parser is not None:
                parser(self.tenant_directory, file, self.directory, self.customer).run()

class FileParser:
    def __init__(
        self,
        tenant_directory: PosixPath,
        file: PosixPath,
        directory: str,
        customer: str,
    ):
        self.tenant_directory = tenant_directory
        self.file = file
        self.directory = directory
        self.customer = customer
        self.core_directory_path = tenant_directory / f'dbt_packages/{MASTER_PROJECT_NAME}/{directory}'
        
    @property
    def file_contents(self):
        return self.file.read_text()
    
    @property
    def file_name(self):
        return f'{self.customer}_{self.file.name}'
    
    @property
    def file_path(self):
        return self.tenant_directory.joinpath(
            self.directory,
            self.customer, 
            *self.file.relative_to(self.core_directory_path).parts[:-1]
        )
        
    @property
    def file_path_and_file(self):
        return self.file_path / self.file_name
    
    
    def after_file_contents(self):
        return ''
        
    def before_file_contents(self):
        return ''
    
    def write_file_contents(self, encoding='utf-8'):
        before = self.before_file_contents()
        file_contents = self.file_contents
        after = self.after_file_contents()
        all_contents = before + file_contents + after
        with self.file_path_and_file.open('w', encoding=encoding) as f:
            f.write(all_contents)
            
    def run(self):
        self.file_path.mkdir(parents=True, exist_ok=True)
        self.write_file_contents()


class SchemaParser(FileParser):
    def __init__(
        self,
        tenant_directory: PosixPath,
        file: PosixPath,
        resource: str,
        customer: str,
    ):
        super().__init__(tenant_directory, file, resource, customer)
        
    @property
    def file_name(self):
        return self.file.name
    
    @property
    def file_contents(self):
        with self.file.open() as fp:
            data = yaml.safe_load(fp)
        
        if any([i in data for i in ['models', 'seeds', 'snapshots']]):
            data = self._modify_yml_for_models(data)
        if 'sources' in data:
            data = self._modify_yml_for_sources(data)
        return data
            
    def _modify_yml_for_models(self, data: Dict, key='models'):
        items = data[key]
        for item in items:
            item['name'] = f"{self.customer}_{item['name']}"
        return data
    
    def _modify_yml_for_sources(self, data: Dict):
        sources = data['sources']
        for source in sources:
            source['schema'] = self.customer
        return data
    
    def write_file_contents(self, encoding='utf-8'):
        data = self.file_contents
        with self.file_path_and_file.open('w', encoding=encoding) as f:
            yaml.dump(data, f, sort_keys=False)


class MacroParser(FileParser):
    def __init__(
        self,
        tenant_directory: PosixPath,
        file: PosixPath,
        resource: str,
        customer: str,
    ):
        super().__init__(tenant_directory, file, resource, customer)
        
    @property
    def file_name(self):
        return self.file.name
        
    @property
    def file_path(self):
        """Each tenant can leverage the same macro"""
        return self.tenant_directory.joinpath(
            self.directory, 
            *self.file.relative_to(self.core_directory_path).parts[:-1]
        )
        
        
class DocParser(MacroParser):
    def __init__(
        self,
        tenant_directory: PosixPath,
        file: PosixPath,
        resource: str,
        customer: str,
    ):
        super().__init__(tenant_directory, file, resource, customer)
        
    @property
    def file_path(self):
        """Each tenant can leverage the same docs"""
        return self.tenant_directory.joinpath(self.directory, 'shared')


class ModelParser(FileParser):
    def __init__(
        self,
        tenant_directory: PosixPath,
        file: PosixPath,
        resource: str,
        customer: str,
    ):
        super().__init__(tenant_directory, file, resource, customer)
        
    def before_file_contents(self):
        return f"{{{{ config(schema='{self.customer}', alias='{self.file.stem}') }}}}\n\n"




if __name__ == '__main__':
    path = Path(__file__).parents[1]

    tenant_directories = [d for d in path.iterdir() if d.is_dir() and 'tenants' in d.name]

    for tenant_directory in tenant_directories:
        
        # Retrieve the customers in the environment    
        customers_in_environment = CLUSTER_TENANT_DICT[tenant_directory.name]
        for customer in customers_in_environment:
            
            for directory in DIRECTORIES:
            
                ParseDirectory(tenant_directory, directory, customer).run()


"""
TODO: Should there be a way to ignore stuff?
TODO: Create a github action (P0)
TODO: Need to pull stuff out of the main dbt_project.yml file.  Need to alter at least the models config and change the project name underneath
      as well as adding the same paths to each tenant (P0)
TODO: Think of an appropriate workflow for this -> How are you making changes to core and then updating each of the environments? (P0)
TODO: Pinning to a release is great and all as far as the code you return, but the database has already been changed to something else
      Think about this in the context of the above point.  What are some ways that we can build this (i.e. I make a breaking change
      to my core models, but I build them in different tables.  That way I ensure proper deprecation.)  HOW CAN I DO THIS PROGRAMMATICALLY?
TODO: Change the core project to inclue the config piece.  The before/after is a bit hacky (P1)
TODO: How do exposures work in this world?  Do they?  (P3)

"""
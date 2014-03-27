import unittest, luigi, tempfile, os 
from luigi import LocalTarget
from mortar.luigi.shellscript import ShellScriptTask

class TestShellScriptTask(ShellScriptTask):
    def subprocess_commands(self):
        return 'echo hello-world; echo hello-world-again;'
        
class TestShellScript(unittest.TestCase):

    def setUp(self):
        self.token_path = tempfile.mkdtemp()


    def tearDown(self):
        path = self.t.output()[0].path
        if os.path.isfile(path): 
            os.remove(path)
    
    def test_output_token(self):
        """
        Output should be the given token path and the class name
        """
        t = TestShellScriptTask(self.token_path) 
        self.t = t 
        luigi.build([t], local_scheduler=True)
        self.assertEquals('%s/%s' % (self.token_path, t.__class__.__name__), t.output()[0].path)

    def test_run_process(self):
        """
        Check output is ran
        """
        t = TestShellScriptTask(self.token_path) 
        self.t = t 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.t.cmd_output['stdout'], 'hello-world\nhello-world-again\n')
        self.assertEquals(self.t.cmd_output['cmd'], 'echo hello-world; echo hello-world-again;')
        self.assertEquals(self.t.cmd_output['stderr'], '')


    def test_run_error_throw(self):
        """
        Raise exception at error
        """
        t = TestShellScriptTask(self.token_path) 
        self.t = t 
        self.assertRaises(RuntimeError, lambda: t._check_error('error', 'error message'))


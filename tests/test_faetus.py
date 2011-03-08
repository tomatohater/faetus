#!/usr/bin/python
import unittest
import os
import sys
import ftplib
import StringIO

from faetus.constants import default_address, default_port


class FaetusTest(unittest.TestCase):
    ''' Faetus main test '''

    def setUp(self):
        if not all(['AWS_ACCESS_KEY_ID' in os.environ,
                    'AWS_SECRET_ACCESS_KEY' in os.environ]):
            print "env AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not found."
            sys.exit(1)

        self.username = os.environ['AWS_ACCESS_KEY_ID']
        self.password = os.environ['AWS_SECRET_ACCESS_KEY']
        self.cnx = ftplib.FTP()
        self.cnx.host = default_address
        self.cnx.port = default_port
        self.cnx.connect()
        self.cnx.login(self.username, self.password)
        self.cnx.mkd("/faetus_testing")
        self.cnx.cwd("/faetus_testing")

    def test_mkdir_chdir_rmdir(self):
        ''' mkdir/chdir/rmdir directory '''
        directory = "/foobarrandom"
        self.assertEqual(self.cnx.mkd(directory), directory)
        self.assertEqual(self.cnx.cwd(directory),
                         '250 "%s" is the current directory.' % (directory))
        self.assertEqual(self.cnx.rmd(directory), "250 Directory removed.")

    def test_write_open_delete(self):
        ''' write/open/delete file '''
        content_string = "Hello Moto"
        self.cnx.storbinary("STOR testfile.txt",
                            StringIO.StringIO(content_string))
        store = StringIO.StringIO()
        self.cnx.retrbinary("RETR testfile.txt", store.write)
        self.assertEqual(store.getvalue(), content_string)
        self.assertEqual(self.cnx.delete("testfile.txt"), "250 File removed.")
        store.close()

    def test_write_to_slash(self):
        ''' write to slash should not be permitted '''
        self.cnx.cwd("/")

        content_string = "Hello Moto"
        try:
            self.cnx.storbinary("STOR testfile.txt",
                                StringIO.StringIO(content_string))
        except(ftplib.error_perm):
            pass
        else:
            self.assert_(False)

    def test_chdir_to_a_file(self):
        ''' chdir to a file '''

        self.cnx.storbinary("STOR testfile.txt",
                            StringIO.StringIO("Hello Moto"))
        #self.assertRaises does not seems to work no idea why but that works
        try:
            self.cnx.cwd("/faetus_testing/testfile.txt")
        except(ftplib.error_perm):
            pass
        else:
            self.assert_(False)

        self.cnx.delete("testfile.txt")

    def test_chdir_to_slash(self):
        ''' chdir to slash '''
        self.cnx.cwd("/")

    def test_listdir(self):
        ''' list directory '''
        content_string = "Hello Moto"
        self.cnx.storbinary("STOR testfile.txt",
                            StringIO.StringIO(content_string))
        self.assertEqual(self.cnx.nlst()[0], "testfile.txt")
        self.cnx.delete("testfile.txt")

    def tearDown(self):
        self.cnx.rmd("/faetus_testing")
        self.cnx.close()

if __name__ == '__main__':
    unittest.main()

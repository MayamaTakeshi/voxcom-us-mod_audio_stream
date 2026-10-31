"""Compile the module's actual command-tokenization block and verify opaque payloads."""
from pathlib import Path
import subprocess
import tempfile
source = (Path(__file__).resolve().parents[1] / 'mod_audio_stream.c').read_text()
block = source[source.index('        /* Split the command prefix'):source.index('\n    }\n    switch_log_printf', source.index('        /* Split the command prefix'))]
program = r'''
#include <assert.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
static void check(const char* command, int count, const char* payload) {
 char *mycmd=strdup(command), *argv[6]={0}; int argc=0;
BLOCK
 assert(argc==count); assert(strcmp(argv[count-1],payload)==0); free(mycmd);
}
int main() {
 check("uuid send_text {\"text\":\"hello world's \\\"quote\\\"\"}",3,"{\"text\":\"hello world's \\\"quote\\\"\"}");
 check("uuid stop final message",3,"final message");
 check("uuid start ws://host mono 16k metadata with spaces",6,"metadata with spaces");
 check("uuid START ws://host mono",4,"mono");
}
'''.replace('BLOCK', block)
with tempfile.TemporaryDirectory() as directory:
    path=Path(directory)
    (path/'test.c').write_text(program)
    subprocess.run(['cc',str(path/'test.c'),'-o',str(path/'test')],check=True)
    subprocess.run([str(path/'test')],check=True)
print('command payloads: PASS')

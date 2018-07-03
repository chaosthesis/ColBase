import os
import time
import timeit
import shutil
import subprocess
import difflib

ROOT_DIR = "./"

EXEC_DIR = os.path.join(ROOT_DIR, "src")
TEST_DIR = os.path.join(ROOT_DIR, "project_tests_1M")
TEST_LOG_DIR = os.path.join(ROOT_DIR, "logs")

CONTROL = [16, 20, 22, 26, 28, 31]
SHUTDOWN = [1, 2, 10, 18, 19, 24, 25, 30]
SKIP = []
CLEAN = "distclean"
START_AT = 1
END_AT = 42

def makefile(args):
    if args:
        subprocess.call(["make", args], cwd=EXEC_DIR, stdout=subprocess.PIPE)
    else:
        subprocess.call(["make"], cwd=EXEC_DIR, stdout=subprocess.PIPE)

def start_server():
    output = open(os.path.join(TEST_LOG_DIR, "output.server"), "w")
    return subprocess.Popen("./server", cwd=EXEC_DIR, stdout=output)

def start_client(i):
    test = "test%02d" % i    

    with open(os.path.join(TEST_DIR, test + ".dsl"), "r") as fin:
        with open(os.path.join(TEST_LOG_DIR, test + ".res"), "w") as fout:
            start_time = timeit.default_timer()
            client = subprocess.Popen(
                "./client", cwd=EXEC_DIR, stdin=fin, stdout=fout)
            client.wait()
            elapsed = timeit.default_timer() - start_time

    explines = [line for line in open(
        os.path.join(TEST_DIR, test + ".exp"), "r")]
    reslines = [line for line in open(
        os.path.join(TEST_LOG_DIR, test + ".res"), "r")]
    explines = filter(lambda x: x.strip(), explines)
    reslines = filter(lambda x: x.strip(), reslines)

    diffs = ""
    for line in difflib.unified_diff(
        sorted(explines), sorted(reslines),
            fromfile='exp', tofile='res', lineterm=''):
        if line.strip():
            diffs += line + "\n"

    if not diffs:
        if i in SHUTDOWN:
            flag = " <load>"
        elif i in CONTROL:
            flag = " <control>"
        else:
            flag = ""
        print "[Pass] %s -> %.3f ms%s" % (test, elapsed*1000, flag)
        return True
    else:
        print "[Fail] %s" % test
        with open(os.path.join(TEST_LOG_DIR, test + ".diff"), "w") as fout:
            fout.write(diffs)
        return False

def main(): 
    print "Compiling..."
    makefile(CLEAN)
    makefile(None)
    
    print "Starting..."
    if os.path.exists(TEST_LOG_DIR):
        shutil.rmtree(TEST_LOG_DIR)
    os.makedirs(TEST_LOG_DIR)

    s = start_server()
    for i in range(START_AT, END_AT):
        if i in SKIP: continue
        passed = start_client(i)
        #if i in CONTROL:
        #    passed = start_client(i)        
        if not passed:
            break
        if i in SHUTDOWN:
            s.wait()
            if s.poll() is None:
                print "Server did not shut down!"
                break
            else:
                s = start_server()
    if s.poll() is None:
        s.terminate()

if __name__ == "__main__":
    main()

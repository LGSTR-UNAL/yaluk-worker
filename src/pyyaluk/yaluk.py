import os
import glob
import subprocess
import time
import logging


def build_libyaluk(source_directory: str):
    os.chdir(source_directory)
    bash_cmd = "make"
    process = subprocess.Popen(bash_cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()


def build_libatp(source_directory: str, pwd: str) -> None:
    os.chdir(source_directory)
    bash_cmd = f"""chmod 755 vardim vardimn; dos2unix vardimn; 
               ./vardimn listsize.ylk; make; cp startup {pwd}"""

    print("copying startup file")
    process = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    time.sleep(5)


def simulate(
    case: str,
    delete_tmp: bool = True,
    case_dir: str = "",
    tpbig: str | None = None,
) -> tuple[float, bytes, int]:
    start_time: float = time.time()
    tpbig = tpbig or os.getenv("TPBIG_PATH", "")
    previous_cwd = os.getcwd()
    os.chdir(case_dir)
    name = "test"

    try:
        logging.info(f"Running stroke")
        for dat_file in glob.glob(f"{case_dir}/CaseFiles/*.dat"):
            os.remove(dat_file)
        bash_cmd = f"{tpbig} DISK case.atp s -r"
        process = subprocess.Popen(
            bash_cmd.split(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output, error = process.communicate(input=bytes(name, "utf-8"))
        if process.returncode != 0:
            logging.error(f"tpbig exited with code {process.returncode}")
        if error:
            logging.error(f"Error running stroke: {error.decode('utf-8', 'replace')}")

        if delete_tmp:
            for tmp_file in glob.glob("*.tmp"):
                os.remove(tmp_file)
            for bin_file in glob.glob("*.bin"):
                os.remove(bin_file)
            for dbg_file in glob.glob("*.dbg"):
                os.remove(dbg_file)
    finally:
        os.chdir(previous_cwd)

    end_time: float = time.time()
    duration: float = end_time - start_time
    logging.info(f"Completed simulation in {duration} s.")
    return duration, error, process.returncode

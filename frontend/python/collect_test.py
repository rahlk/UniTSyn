"""
Collects all the test functions from projects following
"Conventions for Python test discovery" in
https://docs.pytest.org/en/7.4.x/explanation/goodpractices.html#test-discovery
"""

import os
import re
import ast
import fire
import shutil
import tarfile
import tempfile
from pathlib import Path
from collections import Counter
from typing import List

from frontend.util import run_with_timeout, wrap_repo, mp_map_repos, TimeoutException
from navigate import ModuleNavigator, dump_ast_func
from scripts.download_repos import download_repo
from scripts.common import get_access_token
from github import Github, Auth


def collect_test_files(root: str):
    """collect all files in the root folder recursively and filter to match the given patterns"""
    patterns = [
        r".*_test\.py",
        r"test_.*\.py",
    ]
    test_files = []
    for parent, _, files in os.walk(root):
        for file in files:
            if any([re.match(ptn, file) for ptn in patterns]):
                test_files.append(os.path.join(parent, file))
    return test_files


def collect_test_funcs(module_path: str):
    """collect testing functions from the target file"""
    nav = ModuleNavigator(module_path)
    funcs = nav.find_all(ast.FunctionDef)
    # funcs = nav.find_all(lambda x:isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef)))

    def is_test_cls(node: ast.AST):
        """is a test class if
        1.1 class name starts with Test
        1.2 inherit from unittest.TestCase
        2. a static class without a init function
        """
        if not isinstance(node, ast.ClassDef):
            return False
        # if not node.name.startswith('Test'): return False
        test_prefix = node.name.startswith("Test")
        inherit_unittest_attr = any(
            [
                isinstance(base, ast.Attribute) and base.attr == "TestCase"
                for base in node.bases
            ]
        )
        inherit_unittest_name = any(
            [
                isinstance(base, ast.Name) and base.id == "TestCase"
                for base in node.bases
            ]
        )
        if not any([test_prefix, inherit_unittest_name, inherit_unittest_attr]):
            return False
        cls_funcs = nav.find_all(ast.FunctionDef, root=node)
        return not any(func.name == "__init__" for func in cls_funcs)

    def has_assert(func: ast.AST):
        # builtin assertion
        if len(nav.find_all(ast.Assert, root=func)) > 0:
            return True
        # Check for various assertion patterns
        for call in nav.find_all(ast.Call, root=func):
            if isinstance(call.func, ast.Attribute):
                # unittest style: self.assertEqual, self.assertTrue, etc.
                if call.func.attr.startswith("assert"):
                    return True
            elif isinstance(call.func, ast.Name):
                # nose style: assert_equal, assert_true, etc.
                # pytest style: pytest.raises, etc.
                # other direct assertion function calls
                if call.func.id.startswith("assert") or call.func.id in [
                    "raises",
                    "fail",
                    "ok_",
                ]:
                    return True
        return False

    def is_test_outside_cls(func: ast.AST):
        """decide if the function is a testing function outside a class
        return true if its name starts with "test"
        """
        return func.name.startswith("test")

    def is_test_inside_cls(func: ast.AST, path: List[ast.AST]):
        """decide if the function is a testing function inside a class
        return true if its class is prefixed by "Test" and either
        + it is prefixed by "test"
        + it is decorated with @staticmethod and @classmethods
        """
        # keep only the node in path whose name is prefixed by "Test"
        cls_path = [n for n in path if is_test_cls(n)]
        if len(cls_path) == 0:
            return False
        if func.name.startswith("test"):
            return True
        decorators = getattr(func, "decorator_list", [])
        return any(
            isinstance(d, ast.Name) and d.id in ("staticmethod", "classmethods")
            for d in decorators
        )

    test_funcs = []
    for func in funcs:
        path = nav.get_path_to(func)
        is_cls = [isinstance(n, ast.ClassDef) for n in path]
        is_test = False
        is_test |= any(is_cls) and is_test_inside_cls(func, path)
        is_test |= not any(is_cls) and is_test_outside_cls(func)
        is_test &= has_assert(func)
        if not is_test:
            continue
        func_id = dump_ast_func(func, module_path, nav, path)
        test_funcs.append(func_id)

    return test_funcs


@run_with_timeout
def collect_from_repo(
    repo_id: str, repo_root: str, test_root: str, auto_download: bool = True
):
    """collect all test functions in the given project
    return (status, nfile, ntest)
    status can be 0: success, 1: repo not found, 2: test not found, 3: skip when output file existed,
           4: download failed, 5: extract failed
    """
    test_path = os.path.join(test_root, wrap_repo(repo_id) + ".txt")
    # skip if exist
    if os.path.exists(test_path):
        return 3, 0, 0

    repo_path = os.path.join(repo_root, wrap_repo(repo_id))
    extracted_here = False  # Track if we extracted the repo here

    # If repo doesn't exist and auto_download is enabled, download and extract it
    if not os.path.exists(repo_path) or not os.path.isdir(repo_path):
        if not auto_download:
            return 1, 0, 0

        # Check if tarball exists
        tarball_root = os.path.join(os.path.dirname(repo_root), "repos_tarball")
        tarball_path = os.path.join(tarball_root, wrap_repo(repo_id) + ".tar.gz")

        if not os.path.exists(tarball_path):
            # Download the repo
            try:
                # Get access token if available
                oauth_token = get_access_token()
                hub = Github(auth=Auth.Token(oauth_token)) if oauth_token else Github()

                # Ensure tarball directory exists
                os.makedirs(tarball_root, exist_ok=True)

                # Download repo
                status, result = download_repo(
                    hub=hub,
                    repo_id=repo_id,
                    path=tarball_path,
                    fetch_timeout=30,
                    download_timeout=300,
                )

                if status != 0:
                    return 4, 0, 0  # Download failed

            except Exception as e:
                print(f"Download failed for {repo_id}: {e}")
                return 4, 0, 0

        # Extract the tarball
        try:
            # Ensure repo directory exists
            os.makedirs(repo_root, exist_ok=True)

            with tarfile.open(tarball_path, "r:gz") as tar:
                # Extract to a temporary location first
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Use data filter for safer extraction (Python 3.12+)
                    try:
                        tar.extractall(temp_dir, filter="data")
                    except TypeError:
                        # Fallback for older Python versions
                        tar.extractall(temp_dir)

                    # Find the extracted directory (usually has a different name)
                    extracted_dirs = [
                        d
                        for d in os.listdir(temp_dir)
                        if os.path.isdir(os.path.join(temp_dir, d))
                    ]

                    if extracted_dirs:
                        # Move the extracted directory to the correct location
                        source_path = os.path.join(temp_dir, extracted_dirs[0])
                        shutil.move(source_path, repo_path)
                        extracted_here = True
                    else:
                        return 5, 0, 0  # Extract failed

        except Exception as e:
            print(f"Extract failed for {repo_id}: {e}")
            return 5, 0, 0

    try:
        # collect potential testing modules
        all_files = collect_test_files(repo_path)
        test_files, test_funcs = [], []
        for f in all_files:
            try:
                funcs = collect_test_funcs(f)
            except TimeoutException:
                raise
            except Exception:  # pylint: disable=broad-except
                funcs = None
            if funcs is None or len(funcs) == 0:
                continue
            test_files.append(f)
            test_funcs.extend(funcs)

        if len(test_funcs) == 0:
            result = 2, len(test_files), len(test_funcs)
        else:
            # save to disk
            os.makedirs(test_root, exist_ok=True)
            with open(test_path, "w") as outfile:
                for func_id in test_funcs:
                    parts = func_id.split("::")
                    parts[0] = str(
                        Path(os.path.abspath(parts[0])).relative_to(
                            os.path.abspath(repo_root)
                        )
                    )
                    func_id = "::".join(parts)
                    outfile.write(f"{func_id}\n")
            result = 0, len(test_files), len(test_funcs)

    finally:
        # Clean up: remove extracted repo if we extracted it here
        if extracted_here and os.path.exists(repo_path):
            try:
                shutil.rmtree(repo_path)
            except Exception as e:
                print(f"Warning: Failed to cleanup {repo_path}: {e}")

    return result


def main(
    repo_id: str = "ageitgey/face_recognition",
    repo_root: str = "data/repos/",
    test_root: str = "data/tests",
    timeout: int = 120,
    nprocs: int = 0,
    limits: int = -1,
    auto_download: bool = True,
):
    # if repo_id_list is a file then load lines
    # otherwise it is the id of a specific repo
    try:
        repo_id_list = [line.strip() for line in open(repo_id, "r").readlines()]
    except FileNotFoundError:
        repo_id_list = [repo_id]
    if limits > 0:
        repo_id_list = repo_id_list[:limits]
    print(f"Loaded {len(repo_id_list)} repos to be processed")

    status_nfile_ntest = mp_map_repos(
        collect_from_repo,
        repo_id_list=repo_id_list,
        nprocs=nprocs,
        repo_root=repo_root,
        test_root=test_root,
        timeout=timeout,
        auto_download=auto_download,
    )

    filtered_results = [i for i in status_nfile_ntest if i is not None]
    if len(filtered_results) < len(status_nfile_ntest):
        print(f"{len(status_nfile_ntest) - len(filtered_results)} repos timeout")
    status, nfile, ntest = zip(*filtered_results)
    status_counter: Counter[int] = Counter(status)
    print(
        f"Processed {sum(status_counter.values())} repos with {status_counter[3]} skipped, "
        f"{status_counter[1]} not found, {status_counter.get(4, 0)} download failed, "
        f"{status_counter.get(5, 0)} extract failed, and {status_counter[2]} failed to mine any testing functions"
    )
    print(f"Collected {sum(ntest)} tests from {sum(nfile)} files in total")


if __name__ == "__main__":
    fire.Fire(main)

"""
Main orchestration script that downloads repos, runs collect_all, and cleans up.

This script provides a complete automated workflow for processing repositories:
1. Downloads and extracts repositories from GitHub one at a time
2. Runs the collect_all pipeline to gather tests and focal functions
3. Automatically cleans up extracted repositories after processing each one

Usage examples:
    # Process a single repository
    python run_all.py --repo_id="alexras/pylsdj"

    # Process repositories from a file list
    python run_all.py --repo_id="data/repo_meta/python.txt" --limits=10

    # Process with custom timeout and parallel processing
    python run_all.py --repo_id="data/repo_meta/python.txt" --timeout=600 --nprocs=4

The script processes repositories one at a time to minimize disk usage.
All downloaded ZIP files are preserved for future use, but extracted repositories
are automatically removed after processing each one to save disk space.
"""

import os
import sys
import shutil
import zipfile
import tempfile
import requests
from typing import List
import fire
from pathlib import Path

# Add frontend/python to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "frontend", "python"))

from frontend.util import wrap_repo
import frontend.python.collect_all as collect_all


def download_repo_zip(repo_id: str, zip_path: str, timeout: int = 300) -> bool:
    """
    Download a repository as a ZIP file from GitHub.

    Args:
        repo_id: Repository ID in format "owner/repo"
        zip_path: Path where the ZIP file should be saved
        timeout: Download timeout in seconds

    Returns:
        bool: True if download successful, False otherwise
    """
    # Construct GitHub ZIP download URL
    zip_url = f"https://github.com/{repo_id}/archive/refs/heads/main.zip"

    try:
        print(f"Downloading {repo_id} from {zip_url}...")

        # Try main branch first, then master if main fails
        for branch in ["main", "master"]:
            zip_url = f"https://github.com/{repo_id}/archive/refs/heads/{branch}.zip"

            response = requests.get(zip_url, timeout=timeout, stream=True)

            if response.status_code == 200:
                # Ensure parent directory exists
                os.makedirs(os.path.dirname(zip_path), exist_ok=True)

                # Download the file
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"Successfully downloaded {repo_id} ({branch} branch)")
                return True
            elif response.status_code == 404 and branch == "main":
                print(f"Branch 'main' not found for {repo_id}, trying 'master'...")
                continue
            else:
                print(
                    f"Failed to download {repo_id} from {branch} branch: HTTP {response.status_code}"
                )

        return False

    except requests.exceptions.Timeout:
        print(f"Download timeout for {repo_id}")
        return False
    except Exception as e:
        print(f"Download failed for {repo_id}: {e}")
        return False


def extract_repo_zip(zip_path: str, extract_path: str) -> bool:
    """
    Extract a repository ZIP file to the specified location.

    Args:
        zip_path: Path to the ZIP file
        extract_path: Path where the repository should be extracted

    Returns:
        bool: True if extraction successful, False otherwise
    """
    try:
        print(f"Extracting {zip_path}...")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Extract to a temporary location first
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_ref.extractall(temp_dir)

                # Find the extracted directory (usually has a different name like "repo-main")
                extracted_dirs = [
                    d
                    for d in os.listdir(temp_dir)
                    if os.path.isdir(os.path.join(temp_dir, d))
                ]

                if extracted_dirs:
                    # Move the extracted directory to the correct location
                    source_path = os.path.join(temp_dir, extracted_dirs[0])

                    # Ensure parent directory exists
                    os.makedirs(os.path.dirname(extract_path), exist_ok=True)

                    shutil.move(source_path, extract_path)
                    print(f"Extracted to {extract_path}")
                    return True
                else:
                    print(f"Failed to extract {zip_path}: no directories found")
                    return False

    except Exception as e:
        print(f"Extract failed for {zip_path}: {e}")
        return False


def process_single_repository(
    repo_id: str,
    repos_root: str = "data/repos",
    test_root: str = "data/tests",
    focal_root: str = "data/focal",
    zips_root: str = "data/repos_zip",
    timeout: int = 300,
    nprocs: int = 0,
    original_collect_focal: bool = False,
) -> bool:
    """
    Process a single repository: download, extract, process, and cleanup.

    Args:
        repo_id: Repository ID to process
        repos_root: Directory where extracted repositories are stored temporarily
        test_root: Directory where test results are saved
        focal_root: Directory where focal function results are saved
        zips_root: Directory where downloaded ZIP files are stored
        timeout: Timeout in seconds for processing
        nprocs: Number of processes for parallel processing (0 = sequential)
        original_collect_focal: Whether to use original focal collection method

    Returns:
        bool: True if processing successful, False otherwise
    """

    repo_name = wrap_repo(repo_id)
    repo_path = os.path.join(repos_root, repo_name)
    zip_path = os.path.join(zips_root, f"{repo_name}.zip")

    try:
        # Step 1: Download ZIP if not exists
        if not os.path.exists(zip_path):
            if not download_repo_zip(repo_id, zip_path, timeout):
                print(f"Failed to download {repo_id}")
                return False
        else:
            print(f"ZIP already exists for {repo_id}")

        # Step 2: Extract repository
        if not extract_repo_zip(zip_path, repo_path):
            print(f"Failed to extract {repo_id}")
            return False

        # Step 3: Create a temporary repo list file for collect_all
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as tmp_file:
            tmp_file.write(f"{repo_id}\n")
            tmp_repo_file = tmp_file.name

        try:
            # Step 4: Run collect_all on this single repository
            print(f"Processing {repo_id}...")
            collect_all.main(
                repo_id=tmp_repo_file,
                test_root=test_root,
                repo_root=repos_root,
                focal_root=focal_root,
                timeout=timeout,
                nprocs=nprocs,
                original_collect_focal=original_collect_focal,
                limits=-1,
            )

            print(f"Successfully processed {repo_id}")
            return True

        finally:
            # Clean up temporary file
            if os.path.exists(tmp_repo_file):
                os.unlink(tmp_repo_file)

    except Exception as e:
        print(f"Error processing {repo_id}: {e}")
        return False

    finally:
        # Step 5: Clean up extracted repository
        if os.path.exists(repo_path):
            try:
                shutil.rmtree(repo_path)
                print(f"Cleaned up extracted repo: {repo_path}")
            except Exception as e:
                print(f"Warning: Failed to cleanup {repo_path}: {e}")


def process_repositories(
    repo_ids: List[str],
    repos_root: str = "data/repos",
    test_root: str = "data/tests",
    focal_root: str = "data/focal",
    zips_root: str = "data/repos_zip",
    timeout: int = 300,
    nprocs: int = 0,
    original_collect_focal: bool = False,
    limits: int = -1,
):
    """
    Process repositories one at a time with automatic download, extraction, and cleanup.

    Args:
        repo_ids: List of repository IDs to process
        repos_root: Directory where extracted repositories are stored temporarily
        test_root: Directory where test results are saved
        focal_root: Directory where focal function results are saved
        zips_root: Directory where downloaded ZIP files are stored
        timeout: Timeout in seconds for processing each repository
        nprocs: Number of processes for parallel processing (0 = sequential)
        original_collect_focal: Whether to use original focal collection method
        limits: Maximum number of repositories to process (-1 = no limit)
    """

    # Apply limits if specified
    if limits > 0:
        repo_ids = repo_ids[:limits]

    print(f"Processing {len(repo_ids)} repositories one at a time")

    # Ensure directories exist
    os.makedirs(repos_root, exist_ok=True)
    os.makedirs(test_root, exist_ok=True)
    os.makedirs(focal_root, exist_ok=True)
    os.makedirs(zips_root, exist_ok=True)

    successful_count = 0
    failed_repos = []

    # Process each repository individually
    for i, repo_id in enumerate(repo_ids, 1):
        print(f"\n{'='*60}")
        print(f"Processing repository {i}/{len(repo_ids)}: {repo_id}")
        print(f"{'='*60}")

        success = process_single_repository(
            repo_id=repo_id,
            repos_root=repos_root,
            test_root=test_root,
            focal_root=focal_root,
            zips_root=zips_root,
            timeout=timeout,
            nprocs=nprocs,
            original_collect_focal=original_collect_focal,
        )

        if success:
            successful_count += 1
        else:
            failed_repos.append(repo_id)

    # Print summary
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total repositories: {len(repo_ids)}")
    print(f"Successful: {successful_count}")
    print(f"Failed: {len(failed_repos)}")

    if failed_repos:
        print(f"\nFailed repositories:")
        for repo_id in failed_repos:
            print(f"  - {repo_id}")


def main(
    repo_id: str = "ageitgey/face_recognition",
    repos_root: str = "data/repos",
    test_root: str = "data/tests",
    focal_root: str = "data/focal",
    zips_root: str = "data/repos_zip",
    timeout: int = 300,
    nprocs: int = 0,
    original_collect_focal: bool = False,
    limits: int = -1,
):
    """
    Main function to download, process, and cleanup repositories one at a time.

    Args:
        repo_id: Either a repository ID (e.g. "owner/repo") or path to a file containing repo IDs
        repos_root: Directory where extracted repositories are stored temporarily
        test_root: Directory where test results are saved
        focal_root: Directory where focal function results are saved
        zips_root: Directory where downloaded ZIP files are stored
        timeout: Timeout in seconds for processing each repository
        nprocs: Number of processes for parallel processing (0 = sequential)
        original_collect_focal: Whether to use original focal collection method
        limits: Maximum number of repositories to process (-1 = no limit)
    """

    # Parse repo_id input - either single repo or file with repo list
    try:
        repo_ids = [
            line.strip() for line in open(repo_id, "r").readlines() if line.strip()
        ]
        print(f"Loaded {len(repo_ids)} repositories from {repo_id}")
    except FileNotFoundError:
        repo_ids = [repo_id]
        print(f"Processing single repository: {repo_id}")

    # Process the repositories
    process_repositories(
        repo_ids=repo_ids,
        repos_root=repos_root,
        test_root=test_root,
        focal_root=focal_root,
        zips_root=zips_root,
        timeout=timeout,
        nprocs=nprocs,
        original_collect_focal=original_collect_focal,
        limits=limits,
    )

    print("All repositories processed!")


if __name__ == "__main__":
    fire.Fire(main)

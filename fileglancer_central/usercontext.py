import os
import pwd

from loguru import logger


class UserContext:
    def __init__(self, username):
        self.username = username
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._user = None

    def __enter__(self):
        logger.debug(f"Prepare user context for {self.username}")
        user = pwd.getpwnam(self.username)

        uid = user.pw_uid
        gid = user.pw_gid
        gids = os.getgrouplist(self.username, gid)
        try:
            os.setegid(gid)
        except Exception as e:
            logger.error(f"Failed to set the effective gid: {e}")
            raise e

        try:
            os.setgroups(gids)
        except Exception as e:
            logger.error(f"Failed to set the user groups: {e}")
            # reset egid first
            os.setegid(self._gid)
            raise e

        try:
            os.seteuid(uid)
        except Exception as e:
            logger.error(f"Failed to set euid: {e}")
            # reset egid
            os.setegid(self._gid)
            raise e

        self._user = user

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Release user context")
        os.seteuid(self._uid)
        os.setegid(self._gid)
        self._user = None
        return False


def main():
    import sys
    u = sys.argv[1]
    d = sys.argv[2]

    with UserContext(u):
        print(os.listdir(d))
    

if __name__ == "__main__":
    main()
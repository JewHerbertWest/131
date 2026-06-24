import json

from python.core.config.settings import USERS_ROLES_FILE


class UsersRolesRepository:
    def read_users_roles(self) -> dict:
        with open(USERS_ROLES_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def get_user(self, user_id: str) -> dict | None:
        data = self.read_users_roles()
        return data.get("users", {}).get(user_id)

    def get_role(self, role_name: str) -> dict | None:
        data = self.read_users_roles()
        return data.get("roles", {}).get(role_name)

    def is_command_allowed(self, user_id: str, command_type: str) -> bool:
        user = self.get_user(user_id)

        if user is None:
            return False

        return command_type in user.get("allowed_commands", [])

    def is_compartment_allowed(self, user_id: str, compartment_id: str) -> bool:
        user = self.get_user(user_id)

        if user is None:
            return False

        return compartment_id in user.get("allowed_compartments", [])
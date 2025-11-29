/* ===========================================
   AUTH MODULE (shared across all pages)
   =========================================== */

const USERS_KEY = "sitStoreUsers";     // list of registered users
const CURRENT_KEY = "sitStoreUser";    // currently logged-in user

// ---------- GET LIST OF USERS ----------
function getUsers() {
    try {
        return JSON.parse(localStorage.getItem(USERS_KEY)) || [];
    } catch {
        return [];
    }
}

// ---------- SAVE LIST ----------
function saveUsers(users) {
    localStorage.setItem(USERS_KEY, JSON.stringify(users));
}

// ---------- GET CURRENT LOGGED-IN USER ----------
function getCurrentUser() {
    try {
        return JSON.parse(localStorage.getItem(CURRENT_KEY)) || null;
    } catch {
        return null;
    }
}

// ---------- LOGOUT ----------
function logoutUser() {
    localStorage.removeItem(CURRENT_KEY);
}

// ---------- SAVE ACTIVE USER ----------
function setCurrentUser(id, password) {
    localStorage.setItem(CURRENT_KEY, JSON.stringify({ id, password }));
}

// ---------- SHARED HEADER / LOGOUT UI ----------
// Call this from any page (after DOM is ready) to:
//  - show "User: <id>" in #userLabel
//  - wire up #logoutBtn
//  - optionally force login & redirect to login.html
function setupAuthUI(options = {}) {
    const {
        requireLogin = true,
        userBarId = "userBar",
        userLabelId = "userLabel",
        logoutBtnId = "logoutBtn",
    } = options;

    const userBar = document.getElementById(userBarId);
    const userLabel = document.getElementById(userLabelId);
    const logoutBtn = document.getElementById(logoutBtnId);

    const currentUser =
        typeof getCurrentUser === "function" ? getCurrentUser() : null;

    // No user logged in
    if (!currentUser) {
        if (requireLogin) {
            // force login for shop/admin/etc.
            window.location.href = "login.html";
        } else if (userBar) {
            // on public pages, just hide the bar
            userBar.style.display = "none";
        }
        return;
    }

    // Show user id
    if (userLabel) {
        userLabel.textContent = `User: ${currentUser.id}`;
    }

    // Logout behaviour
    if (logoutBtn) {
        logoutBtn.addEventListener("click", (e) => {
            e.preventDefault();
            logoutUser();
            window.location.href = "login.html";
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const toggleTheme = () => {
        const body = document.body;
        const loginContainer = document.querySelector('.login-container');
        const themeIcon = document.getElementById('theme-icon');
        const isDark = body.classList.toggle('dark-theme');
        loginContainer.classList.toggle('dark-theme');
        themeIcon.textContent = isDark ? '☀️' : '🌙';
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
    };

    const themeToggle = document.getElementById('theme-toggle');
    themeToggle.addEventListener('click', toggleTheme);

    const savedTheme = localStorage.getItem('theme');
    const body = document.body;
    const loginContainer = document.querySelector('.login-container');
    const themeIcon = document.getElementById('theme-icon');
    const isDark = savedTheme === 'dark' || savedTheme === null;

    if (isDark) {
        body.classList.add('dark-theme');
        loginContainer.classList.add('dark-theme');
        themeIcon.textContent = '☀️';
        localStorage.setItem('theme', 'dark'); // Сохраняем тёмную тему по умолчанию
    } else {
        themeIcon.textContent = '🌙';
    }
});
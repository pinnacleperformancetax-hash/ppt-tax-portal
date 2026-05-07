PPT MY MESSAGES + YEAR-END FIX BUILD

Full replacement build based on the working client portal package.

Fixes:
- /my/messages actual backend route
- /my/year-end actual backend route
- my_messages.html template
- my_year_end.html template
- client sidebar links
- admin reply/close message routes
- keeps /init database cleanup

After upload:
1. Upload all contents to GitHub.
2. Commit changes.
3. Let Render redeploy.
4. Run: https://ppt-tax-portal.onrender.com/init
5. Test:
   https://ppt-tax-portal.onrender.com/my/messages
   https://ppt-tax-portal.onrender.com/my/year-end

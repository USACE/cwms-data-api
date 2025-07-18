window.addEventListener("load", function () {
    let coll_mob = document.getElementsByClassName("collapsible-mobile");
    for (const col_m_idx = 0; col_m_idx < coll_mob.length; col_m_idx++) {
        coll_mob[col_m_idx].addEventListener("click", function (e) {
            this.classList.toggle("active");
            const content = this.nextElementSibling;
            if (content.style.display == "none") {
                content.style.display = "block";
            } else {
                content.style.display = "none";
            }
        });
    }
}, false);

// Mobile Burger Bar
function openNav() {
    const mobileNav = document.getElementById("mobileNav");
    mobileNav.classList.add("open");
    const child = document.getElementById("mobileNavContent");
    mobileNav.style.right = child.clientWidth - child.offsetWidth + "px";
    const open = document.getElementById("burgerBtn");
    open.style.display = "none";
}

function closeNav() {
    mobileNav.classList.remove("open");
    const open = document.getElementById("burgerBtn");
    open.style.display = null;
}

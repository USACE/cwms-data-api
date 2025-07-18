// Setup the page's collapses
let coll_mob = document.getElementsByClassName("collapsible-mobile");
for (let col_m_idx = 0; col_m_idx < coll_mob.length; col_m_idx++) {
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
const burgerBtn = document.getElementById("burgerBtn");
if (burgerBtn) {
    burgerBtn.addEventListener("click", openNav);
}
const closeBtn = document.querySelector(".closeBtn");
if (closeBtn) {
    closeBtn.addEventListener("click", closeNav);
}

// Mobile Burger Bar
function openNav() {
    const mobileNav = document.getElementById("mobileNav");
    mobileNav.classList.add("open");
    const child = document.getElementById("mobileNavContent");
    mobileNav.style.right = child.clientWidth - child.offsetWidth + "px";
    const BURGER_BTN = document.getElementById("burgerBtn");
    BURGER_BTN.style.display = "none";
}

function closeNav() {
    const mobileNav = document.getElementById("mobileNav")
    mobileNav.classList.remove("open");
    const BURGER_BTN = document.getElementById("burgerBtn");
    BURGER_BTN.style.display = null;
}

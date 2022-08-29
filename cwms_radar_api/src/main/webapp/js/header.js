window.addEventListener("load", function() {
  var coll_mob = document.getElementsByClassName("collapsible-mobile");
  for (let col_m_idx = 0; col_m_idx < coll_mob.length; col_m_idx++) {
    coll_mob[col_m_idx].addEventListener("click", function (e) {
      this.classList.toggle("active");
      var content = this.nextElementSibling;
      
      console.log(content);
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
  var mobileNav = document.getElementById("mobileNav");
  mobileNav.classList.add("open");
  var child = document.getElementById("mobileNavContent");
  mobileNav.style.right = child.clientWidth - child.offsetWidth + "px";
  var open = document.getElementById("burgerBtn");
  open.style.display = "none";
  var child = document.getElementById("overlay-content");
}

function closeNav() {
  var myNav = document.getElementById("mobileNav");
  mobileNav.classList.remove("open");
  var open = document.getElementById("burgerBtn");
  open.style.display = null;
}

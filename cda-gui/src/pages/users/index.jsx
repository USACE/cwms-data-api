import { Badge, Card, H1, H2, Text } from "@usace/groundwork";
import { Link } from "react-router-dom";
import { FaListUl, FaUserShield } from "react-icons/fa";

const pages = [
  {
    title: "User Lists",
    href: "/user-lists",
    icon: FaListUl,
    badge: "Recipients",
    description:
      "Create office-owned lists and manage their membership for notifications and other CDA applications.",
  },
  {
    title: "User Roles",
    href: "/user-roles",
    icon: FaUserShield,
    badge: "Access",
    description:
      "Review staff in an authorized office and assign or remove their CWMS roles.",
  },
];

export default function Users() {
  return (
    <section className="pb-12">
      <div className="mb-8 border-b border-zinc-200 pb-6">
        <Badge color="blue">CDA administration</Badge>
        <H1 className="mt-3">Users</H1>
        <Text className="mt-2 max-w-3xl">
          Manage reusable user groups and office-scoped access from one place. Your
          signed-in CWMS permissions determine which information and actions are
          available.
        </Text>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        {pages.map(({ title, href, icon: Icon, badge, description }) => (
          <Link key={href} className="group block" to={href}>
            <Card className="h-full border-zinc-200 p-6 transition group-hover:border-blue-400 group-hover:shadow-md">
              <div className="mb-5 flex items-start justify-between gap-4">
                <div className="rounded-xl bg-blue-50 p-4 text-blue-700">
                  <Icon aria-hidden="true" className="h-7 w-7" />
                </div>
                <Badge color="zinc">{badge}</Badge>
              </div>
              <H2 className="text-2xl text-blue-800 group-hover:underline">{title}</H2>
              <Text className="mt-2">{description}</Text>
              <Text className="mt-5 font-semibold text-blue-700">Open {title} →</Text>
            </Card>
          </Link>
        ))}
      </div>
    </section>
  );
}

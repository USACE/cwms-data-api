import { UsaceBox } from "@usace/groundwork";

export default function Disclaimer() {
  return (
    <article className="mx-auto max-w-4xl py-8 space-y-6">
      <h1 className="text-3xl font-bold">Disclaimer</h1>
      <UsaceBox title="Data disclaimer">
        <p className="mb-4">
          NOTICE: All data contained herein is preliminary in nature and therefore
          subject to change. The data is for general information purposes ONLY and SHALL
          NOT be used in technical applications such as, but not limited to, studies or
          designs. All critical data should be obtained from and verified by the United
          States Army Corps of Engineers.
        </p>
        <p className="mb-4">
          The United States Government assumes no liability for the completeness or
          accuracy of the data contained herein and any use of such data inconsistent
          with this disclaimer shall be solely at the risk of the user.
        </p>
        <p>
          These are automated systems. Data may be delayed, missing, or incorrect, and
          values may be revised after review. Verify critical data with the USACE office
          responsible for the data.
        </p>
      </UsaceBox>
      <section id="external-links" className="scroll-mt-32">
        <UsaceBox title="External link disclaimer">
          <p className="mb-4">
            The appearance of external hyperlinks does not constitute endorsement by
            United States Army Corps of Engineers (USACE) of the linked websites, or the
            information, products or services contained therein. For other than
            authorized activities such as military exchanges and Morale, Welfare and
            Recreation (MWR) sites, USACE does not exercise any editorial control over
            the information you may find at these locations.
          </p>
          <p>
            Such links are provided consistent with the stated purpose of this website.
          </p>
        </UsaceBox>
      </section>
    </article>
  );
}

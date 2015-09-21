from __future__ import print_function
import xmlrpclib
import getpass
import argparse

username = getpass.getuser()


def stagging(username, url_list):
    """
    Connect to the xmlrpc service at the webportal of ASTRON and stage 
    the selected list of URLs.
    """
    proxy = xmlrpclib.ServerProxy("https://webportal.astron.nl/service/xmlrpc")
    proxy.LtaStager.add(username, url_list)

def check_srm(line):
    """
    Checks if the line looks correct
    Used to filter the posible input from a file
    """
    if (line.startswith("srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/") and 
        line.endswith(".tar")):        
        return True
    else:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stage a list of URL')
    parser.add_argument('file', type=argparse.FileType('r'),
                        help='File with the SRM URLs')
    parser.add_argument('-u', '--user', default=username, 
                        help='User used to stage the data')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False, 
                        help='Only list the files to be staged')
    args = parser.parse_args()
    
    #l = ["srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_233805/L233805_SAP000_SB000_uv.MS.tar",
         #"srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229064_002/L229064_SAP000_SB003_uv.MS.dppp.tar",
         #"srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229064_002/L229064_SAP000_SB000_uv.MS.dppp.tar",
         #"srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229065_002/L229065_SAP000_SB000_uv.MS.dppp.tar",
         #"srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229065/L229065_SAP000_SB000_uv.MS.tar"]
    
    l = filter(check_srm, list(line.strip() for line in args.file))
    
    if not args.dry_run:
        stagging(args.user, l)
    else:
        print(l) 